package io.spotflow.ble.cloud

import android.database.sqlite.SQLiteDatabase
import androidx.test.core.app.ApplicationProvider
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class MessageStoreTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private lateinit var store: MessageStore

    /** A payload whose every byte equals [marker], so survivors are identifiable. */
    private fun payload(marker: Int, size: Int) = ByteArray(size) { marker.toByte() }

    /** Stores one message using the marker as the sequence number, so seq == marker. */
    private fun put(device: String, marker: Int, size: Int) =
        store.enqueueAll(device, listOf(MessageStore.Entry(marker.toLong(), "t", payload(marker, size))))

    /** Sizes are payload bytes only unless a test asks for the per-row overhead. */
    private fun open(maxBytes: Long, rowOverhead: Long = 0) =
        MessageStore(context, maxBytes, rowOverhead).also { store = it }

    @Before
    fun setUp() {
        context.databaseList().forEach { context.deleteDatabase(it) }
    }

    @After
    fun tearDown() {
        if (::store.isInitialized) store.close()
    }

    @Test
    fun `fifo order and removal per device`() {
        open(maxBytes = 10_000)
        put("a", 1, 4); put("b", 2, 4); put("a", 3, 4)

        assertEquals(1L, store.peek("a")!!.seq)
        store.remove("a", 1)
        assertEquals(3L, store.peek("a")!!.seq)
        assertEquals("devices are separate queues", 2L, store.peek("b")!!.seq)
    }

    @Test
    fun `bytes accounting per device and in total`() {
        open(maxBytes = 10_000)
        put("a", 1, 100); put("b", 2, 50)
        assertEquals(100L, store.bytes("a"))
        assertEquals(150L, store.bytes)

        store.remove("a", 1)
        assertEquals(0L, store.bytes("a"))
        assertTrue(store.isEmpty("a"))
        assertEquals(50L, store.bytes)
    }

    @Test
    fun `the limit is shared and the largest device is evicted first`() {
        open(maxBytes = 100)
        put("quiet", 1, 20)
        put("busy", 2, 40)
        put("busy", 3, 40) // 100: at the limit
        val evicted = put("busy", 4, 40) // 140 > 100 -> busy (120) loses its oldest

        assertEquals(setOf("busy"), evicted)
        assertEquals("the quiet device keeps its data", 1L, store.peek("quiet")!!.seq)
        assertEquals(3L, store.peek("busy")!!.seq)
        assertEquals(100L, store.bytes)
    }

    @Test
    fun `single payload larger than the limit is kept alone`() {
        open(maxBytes = 100)
        put("a", 9, 200)
        assertEquals(200L, store.bytes)
        assertEquals(9L, store.peek("a")!!.seq)
    }

    @Test
    fun `survives close and reopen and reports maxSeq per device`() {
        open(maxBytes = 10_000)
        put("a", 7, 30)
        put("b", 2, 30)
        assertEquals(7L, store.maxSeq("a"))
        store.close()

        open(maxBytes = 10_000)
        assertEquals(7L, store.peek("a")!!.seq)
        assertEquals(30L, store.bytes("a"))
        assertEquals(7L, store.maxSeq("a"))
        assertEquals(0L, store.maxSeq("nobody"))
        assertEquals(setOf("a", "b"), store.storedDeviceIds().toSet())
    }

    @Test
    fun `zero-length payloads count as stored`() {
        open(maxBytes = 10_000)
        store.enqueueAll(
            "a",
            listOf(MessageStore.Entry(1, "t", ByteArray(0)), MessageStore.Entry(2, "t", ByteArray(3))),
        )
        store.remove("a", 2)
        assertEquals(0L, store.bytes("a"))
        assertFalse(store.isEmpty("a"))
        assertEquals(listOf("a"), store.storedDeviceIds())
        store.remove("a", 1)
        assertTrue(store.isEmpty("a"))
    }

    @Test
    fun `a duplicate sequence number is ignored, not counted`() {
        open(maxBytes = 10_000)
        put("a", 1, 10)
        put("a", 1, 10)
        assertEquals(10L, store.bytes("a"))
    }

    @Test
    fun `buffers from per-device files of earlier versions are migrated`() {
        legacyFile("spotflow_buffer_dev-1.db", deviceIdInMeta = "dev-1", rows = listOf(5L to 2, 6L to 3))
        legacyFile("spotflow_buffer_dev-2.db", deviceIdInMeta = null, rows = listOf(1L to 4)) // pre-meta
        legacyFile("spotflow_buffer_x~0123456789abcdef.db", deviceIdInMeta = null, rows = listOf(1L to 1))

        open(maxBytes = 10_000)

        assertEquals(setOf("dev-1", "dev-2"), store.storedDeviceIds().toSet())
        assertEquals(5L, store.peek("dev-1")!!.seq)
        assertEquals(5L, store.bytes("dev-1"))
        assertEquals(4L, store.bytes("dev-2"))
        assertEquals(
            "legacy files are removed",
            listOf("spotflow_buffer.db"),
            context.databaseList().filter { it.endsWith(".db") },
        )
    }

    @Test
    fun `peek on an empty store returns null`() {
        open(maxBytes = 10_000)
        assertNull(store.peek("a"))
        assertEquals(0L, store.bytes)
        assertTrue(store.storedDeviceIds().isEmpty())
    }

    @Test
    fun `creating the store does no disk I-O, the database opens on first use`() {
        open(maxBytes = 10_000)
        val file = context.getDatabasePath("spotflow_buffer.db")
        assertFalse("not opened by the constructor (it may run on the main thread)", file.exists())
        assertEquals("the total reads 0 without opening", 0L, store.bytes)
        assertFalse(file.exists())

        store.open()
        assertTrue(file.exists())
        assertTrue(store.available.value)
    }

    @Test
    fun `a database that cannot be opened makes the store unavailable instead of throwing`() {
        context.getDatabasePath("spotflow_buffer.db").mkdirs() // a directory in the file's place
        open(maxBytes = 10_000)

        assertNull("writes are refused", put("a", 1, 10))
        assertFalse(store.available.value)
        assertTrue(store.storedDeviceIds().isEmpty())
        assertNull(store.peek("a"))
        assertEquals("sequence numbers still work, for the RAM tier", listOf(1L, 2L), listOf(store.nextSeq("a"), store.nextSeq("a")))
    }

    @Test
    fun `a batch is read in order and removed in one go`() {
        open(maxBytes = 10_000)
        put("a", 1, 10); put("a", 2, 10); put("a", 3, 10); put("b", 4, 10)

        assertEquals(listOf(1L, 2L), store.peekBatch("a", maxRows = 2, maxPayloadBytes = 1_000).map { it.seq })
        assertEquals("the payload limit ends the batch early", 2, store.peekBatch("a", maxRows = 10, maxPayloadBytes = 25).size)
        assertEquals("but never below one row", 1, store.peekBatch("a", maxRows = 10, maxPayloadBytes = 1).size)

        store.removeAll("a", listOf(1, 2, 99)) // 99 is not stored: skipped
        assertEquals(3L, store.peek("a")!!.seq)
        assertEquals(10L, store.bytes("a"))
        assertEquals("other devices are untouched", 10L, store.bytes("b"))
    }

    @Test
    fun `sizes include the per-row overhead, also after reopening`() {
        open(maxBytes = 10_000, rowOverhead = 20)
        put("a", 1, 5); put("a", 2, 5)
        assertEquals(50L, store.bytes("a"))
        store.remove("a", 1)
        assertEquals(25L, store.bytes)
        store.close()

        open(maxBytes = 10_000, rowOverhead = 20)
        assertEquals(25L, store.bytes("a"))
    }

    @Test
    fun `the limit counts the overhead too`() {
        open(maxBytes = 100, rowOverhead = 30)
        put("a", 1, 10); put("a", 2, 10) // 80
        val evicted = put("a", 3, 10) // 120 > 100
        assertEquals(setOf("a"), evicted)
        assertEquals(2L, store.peek("a")!!.seq)
    }

    @Test
    fun `a gateway without a flash tier does not shrink the limit of the one still writing`() {
        val shared = MessageStore.acquire(context, maxBytes = 10_000) // the running gateway
        store = shared
        shared.enqueueAll("a", listOf(MessageStore.Entry(1, "t", payload(1, 100))))

        // Restarted in place with flash set to 0: the new gateway shares the store.
        assertTrue(MessageStore.acquire(context, maxBytes = 0) === shared)
        // The old gateway now writes its RAM out. Under a limit of 0 this would evict all but one row.
        val evicted = shared.enqueueAll("a", listOf(MessageStore.Entry(2, "t", payload(2, 100))))

        assertTrue(evicted!!.isEmpty())
        assertEquals("the backlog is kept for the new gateway to deliver", 2L, shared.count("a"))
        MessageStore.release(shared)
        MessageStore.release(shared)
    }

    private fun legacyFile(name: String, deviceIdInMeta: String?, rows: List<Pair<Long, Int>>) {
        val file = context.getDatabasePath(name).also { it.parentFile?.mkdirs() }
        SQLiteDatabase.openOrCreateDatabase(file, null).use { db ->
            db.execSQL("CREATE TABLE q (seq INTEGER PRIMARY KEY, topic TEXT NOT NULL, payload BLOB NOT NULL, len INTEGER NOT NULL)")
            for ((seq, size) in rows) {
                db.execSQL("INSERT INTO q VALUES (?, 't', ?, ?)", arrayOf(seq, ByteArray(size), size))
            }
            if (deviceIdInMeta != null) {
                db.execSQL("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
                db.execSQL("INSERT INTO meta VALUES ('device_id', ?)", arrayOf(deviceIdInMeta))
            }
        }
    }
}

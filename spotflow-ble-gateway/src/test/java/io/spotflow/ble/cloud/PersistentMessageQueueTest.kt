package io.spotflow.ble.cloud

import androidx.test.core.app.ApplicationProvider
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class PersistentMessageQueueTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private lateinit var queue: PersistentMessageQueue

    /** A payload whose every byte equals [marker], so survivors are identifiable. */
    private fun payload(marker: Int, size: Int) = ByteArray(size) { marker.toByte() }

    /** Enqueue using the marker as the sequence number, so seq == marker. */
    private fun put(marker: Int, size: Int) = queue.enqueue(marker.toLong(), "t", payload(marker, size))

    private fun open(maxBytes: Long, device: String = "dev") =
        PersistentMessageQueue(context, device, maxBytes).also { queue = it }

    @Before
    fun setUp() {
        context.deleteDatabase("spotflow_buffer_dev.db")
    }

    @After
    fun tearDown() {
        if (::queue.isInitialized) queue.close()
    }

    @Test
    fun `fifo order and removal`() {
        open(maxBytes = 10_000)
        put(1, 4); put(2, 4); put(3, 4)

        val first = queue.peek()!!
        assertEquals(1L, first.seq)
        queue.remove(first.seq)
        assertEquals(2L, queue.peek()!!.seq)
    }

    @Test
    fun `bytes accounting tracks enqueue and remove`() {
        open(maxBytes = 10_000)
        put(1, 100); put(2, 50)
        assertEquals(150L, queue.bytes)

        queue.remove(queue.peek()!!.seq)
        assertEquals(50L, queue.bytes)
    }

    @Test
    fun `eviction drops oldest to stay within cap`() {
        open(maxBytes = 100)
        put(1, 40); put(2, 40); put(3, 40) // 120 > 100 -> evict seq 1

        assertEquals(80L, queue.bytes)
        assertEquals(2L, queue.peek()!!.seq)

        put(4, 40) // evict seq 2
        assertEquals(80L, queue.bytes)
        assertEquals(3L, queue.peek()!!.seq)
    }

    @Test
    fun `single payload larger than cap is kept alone`() {
        open(maxBytes = 100)
        put(9, 200)
        assertEquals(200L, queue.bytes)
        assertEquals(9L, queue.peek()!!.seq)
    }

    @Test
    fun `survives close and reopen and reports maxSeq`() {
        open(maxBytes = 10_000)
        queue.enqueue(7L, "cfg", payload(7, 30))
        assertEquals(7L, queue.maxSeq())
        queue.close()

        open(maxBytes = 10_000)
        val entry = queue.peek()!!
        assertEquals("cfg", entry.topic)
        assertEquals(7L, entry.seq)
        assertEquals(30L, queue.bytes)
        assertEquals(7L, queue.maxSeq())
    }

    @Test
    fun `batch enqueue and row count include zero-length payloads`() {
        open(maxBytes = 10_000)
        queue.enqueueAll(listOf(PersistentMessageQueue.Entry(1, "t", ByteArray(0)), PersistentMessageQueue.Entry(2, "t", ByteArray(3))))
        assertEquals(3L, queue.bytes)
        queue.remove(2)
        assertEquals(0L, queue.bytes)
        assertEquals(false, queue.isEmpty)
        queue.remove(1)
        assertEquals(true, queue.isEmpty)
    }

    @Test
    fun `upgrading a v2 database keeps buffered data`() {
        context.getDatabasePath("spotflow_buffer_dev.db").also { it.parentFile?.mkdirs() }
        android.database.sqlite.SQLiteDatabase.openOrCreateDatabase(context.getDatabasePath("spotflow_buffer_dev.db"), null).use { db ->
            db.execSQL("CREATE TABLE q (seq INTEGER PRIMARY KEY, topic TEXT NOT NULL, payload BLOB NOT NULL, len INTEGER NOT NULL)")
            db.execSQL("INSERT INTO q VALUES (5, 't', x'0102', 2)")
            db.version = 2
        }
        open(maxBytes = 10_000)
        assertEquals(5L, queue.peek()!!.seq)
        assertEquals(2L, queue.bytes)
        assertEquals(listOf("dev"), PersistentMessageQueue.storedDeviceIds(context))
    }

    @Test
    fun `distinct device ids never share a file`() {
        val a = PersistentMessageQueue.databaseName("a.b")
        val b = PersistentMessageQueue.databaseName("a_b")
        assertEquals("spotflow_buffer_a_b.db", b)
        assertEquals(false, a == b)
        val long1 = PersistentMessageQueue.databaseName("x".repeat(70) + "1")
        val long2 = PersistentMessageQueue.databaseName("x".repeat(70) + "2")
        assertEquals(false, long1 == long2)
    }

    @Test
    fun `peek on empty queue returns null`() {
        open(maxBytes = 10_000)
        assertNull(queue.peek())
        assertEquals(0L, queue.bytes)
        assertEquals(0L, queue.maxSeq())
    }
}

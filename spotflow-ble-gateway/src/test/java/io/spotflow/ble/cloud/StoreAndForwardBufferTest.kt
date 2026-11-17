package io.spotflow.ble.cloud

import androidx.test.core.app.ApplicationProvider
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class StoreAndForwardBufferTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private lateinit var store: MessageStore
    private lateinit var pool: BufferPool
    private lateinit var buffer: StoreAndForwardBuffer

    private fun payload(marker: Int, size: Int = 40) = ByteArray(size) { marker.toByte() }

    /** Sizes are payload bytes only unless a test asks for the per-message overheads. */
    private fun open(
        ramMaxBytes: Long,
        diskMaxBytes: Long = 10_000,
        spill: Boolean = true,
        itemOverhead: Long = 0,
        rowOverhead: Long = 0,
    ) {
        store = MessageStore(context, diskMaxBytes, rowOverhead)
        pool = BufferPool(store, ramMaxBytes, flashMaxBytes = if (spill) diskMaxBytes else 0, itemOverhead)
        buffer = pool.open("dev")
    }

    /** Drains everything in order, returning the marker of each item. */
    private fun StoreAndForwardBuffer.drainMarkers(): List<Int> {
        val out = mutableListOf<Int>()
        while (true) {
            val item = takeNext() ?: break
            out += item.payload[0].toInt()
            remove(item)
        }
        return out
    }

    @Before
    fun setUp() {
        context.databaseList().forEach { context.deleteDatabase(it) }
    }

    @After
    fun tearDown() {
        if (::store.isInitialized) store.close()
    }

    @Test
    fun `stays in RAM under the budget`() {
        open(ramMaxBytes = 100)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2)) // 80 bytes, under budget

        assertEquals("flash must be untouched", 0L, store.bytes)
        assertEquals(80L, buffer.bytes)
        assertEquals(listOf(1, 2), buffer.drainMarkers())
    }

    @Test
    fun `spills oldest to flash in one batch down to half the budget`() {
        open(ramMaxBytes = 100)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2))
        buffer.enqueue("t", payload(3)) // 120 > 100 -> spill markers 1 and 2 (down to <= 50 in RAM)

        assertEquals(80L, buffer.diskBytes)
        assertEquals(40L, buffer.ramBytes)
        assertEquals(120L, buffer.bytes)
        assertEquals("flash (oldest) drained first", 1, buffer.takeNext()!!.payload[0].toInt())
    }

    @Test
    fun `the RAM budget is shared and the largest device spills first`() {
        open(ramMaxBytes = 100)
        val quiet = pool.open("quiet")
        var quietChanged = false
        quiet.onExternalChange = { quietChanged = true }

        quiet.enqueue("t", payload(9, size = 20))
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2)) // 100 in RAM across both devices: at the budget
        buffer.enqueue("t", payload(3)) // 140 -> the busy device (120) spills, down to <= 50

        assertEquals("the quiet device stays in RAM", 20L, quiet.ramBytes)
        assertEquals(0L, quiet.diskBytes)
        assertTrue(pool.ramBytes <= 50)
        assertTrue("not touched, so not notified", !quietChanged)
        assertEquals(listOf(1, 2, 3), buffer.drainMarkers())
        assertEquals(listOf(9), quiet.drainMarkers())
    }

    @Test
    fun `a busy device spilling into full flash evicts its own data, not a quiet device's`() {
        open(ramMaxBytes = 40, diskMaxBytes = 100)
        val quiet = pool.open("quiet")
        quiet.enqueue("t", payload(9, size = 30))
        quiet.flushToDisk() // the quiet device has 30 bytes in flash

        repeat(6) { buffer.enqueue("t", payload(it + 1)) } // the busy device keeps spilling

        assertEquals("the quiet device keeps its flash data", 30L, quiet.diskBytes)
        assertTrue("flash stays within its limit", store.bytes <= 100)
        assertEquals(listOf(9), quiet.drainMarkers())
        assertEquals("the busy device lost its oldest", 6, buffer.drainMarkers().last())
    }

    @Test
    fun `a device losing data to another device's spill is notified`() {
        open(ramMaxBytes = 40, diskMaxBytes = 60)
        val other = pool.open("other")
        other.enqueue("t", payload(9, size = 50))
        other.flushToDisk() // other holds the most flash
        var otherChanged = false
        other.onExternalChange = { otherChanged = true }

        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2)) // spills 40 bytes: flash 90 > 60 -> other (50) is evicted

        assertTrue(otherChanged)
        assertEquals(0L, other.diskBytes)
    }

    @Test
    fun `zero-length messages keep FIFO order across tiers`() {
        open(ramMaxBytes = 0)
        buffer.enqueue("a", ByteArray(0))
        buffer.enqueue("b", ByteArray(1))
        buffer.enqueue("c", ByteArray(0))

        val topics = mutableListOf<String>()
        while (true) {
            val item = buffer.takeNext() ?: break
            topics += item.topic
            buffer.remove(item)
        }
        assertEquals(listOf("a", "b", "c"), topics)
    }

    @Test
    fun `without flash the pool is RAM-only and drops the largest device's oldest`() {
        open(ramMaxBytes = 100, spill = false)
        val quiet = pool.open("quiet")
        quiet.enqueue("t", payload(9, size = 20))
        repeat(4) { buffer.enqueue("t", payload(it + 1)) } // 180 bytes > 100

        assertEquals(0L, store.bytes)
        buffer.flushToDisk() // no-op
        assertEquals(0L, store.bytes)
        assertEquals(listOf(9), quiet.drainMarkers())
        assertEquals(listOf(3, 4), buffer.drainMarkers())
    }

    @Test
    fun `preserves FIFO across tiers`() {
        open(ramMaxBytes = 100) // holds ~2 items; the rest spill to flash
        repeat(6) { buffer.enqueue("t", payload(it + 1)) }
        assertTrue("some should have spilled", buffer.diskBytes > 0)
        assertEquals(listOf(1, 2, 3, 4, 5, 6), buffer.drainMarkers())
    }

    @Test
    fun `counts waiting messages and reports gateway-wide usage`() {
        open(ramMaxBytes = 100, diskMaxBytes = 1_000)
        val other = pool.open("other")
        other.enqueue("t", payload(9, size = 10))
        repeat(3) { buffer.enqueue("t", payload(it + 1)) } // 130 in RAM -> the busy device spills

        assertEquals("across both tiers", 3L, buffer.count)
        assertEquals(1L, other.count)
        val usage = pool.usage.value
        assertEquals(130L, usage.ramBytes + usage.flashBytes)
        assertEquals(100L, usage.ramMaxBytes)
        assertEquals(1_000L, usage.flashMaxBytes)

        buffer.drainMarkers()
        assertEquals(0L, buffer.count)
        assertEquals(10L, pool.usage.value.ramBytes)
        assertEquals(0L, pool.usage.value.flashBytes)
    }

    @Test
    fun `takeNext only peeks`() {
        open(ramMaxBytes = 1000)
        buffer.enqueue("t", payload(7))

        val a = buffer.takeNext()!!
        val b = buffer.takeNext()!! // same item — peek does not remove
        assertEquals(a.seq, b.seq)
        buffer.remove(a)
        assertNull(buffer.takeNext())
    }

    @Test
    fun `remove finalizes an item spilled to flash mid-publish`() {
        open(ramMaxBytes = 40) // any second item forces the first to spill
        buffer.enqueue("t", payload(1))
        val inflight = buffer.takeNext()!! // peeked from RAM (seq 1)
        assertEquals(1, inflight.payload[0].toInt())

        // While "publishing", more data arrives and spills the in-flight item to flash.
        buffer.enqueue("t", payload(2))
        assertTrue(buffer.diskBytes > 0)

        // Finalizing by sequence must remove it from flash (no duplicate, no reorder).
        buffer.remove(inflight)
        assertEquals(listOf(2), buffer.drainMarkers())
    }

    @Test
    fun `flushToDisk moves RAM items to flash in order, and a reopened buffer continues after them`() {
        open(ramMaxBytes = 10_000)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2))
        assertEquals(0L, buffer.diskBytes)

        buffer.flushToDisk()
        buffer.close()
        assertEquals(0L, pool.ramBytes)

        val reopened = pool.open("dev")
        reopened.enqueue("t", payload(3))
        assertEquals(listOf(1, 2, 3), reopened.drainMarkers())
    }

    @Test
    fun `a restarted gateway never reuses the sequence numbers of one still flushing`() {
        open(ramMaxBytes = 10_000)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2)) // the old gateway still holds these in RAM

        val restarted = BufferPool(store, 10_000, flashMaxBytes = 10_000, itemOverheadBytes = 0).open("dev")
        restarted.enqueue("t", payload(3))
        buffer.flushToDisk() // the old gateway flushes only after the new one started
        buffer.close()
        restarted.flushToDisk()

        assertEquals("nothing lost to a sequence-number clash", listOf(1, 2, 3), restarted.drainMarkers())
    }

    @Test
    fun `the pool is released once shut down and every buffer has closed`() {
        store = MessageStore(context, 10_000)
        var released = false
        pool = BufferPool(store, 100, flashMaxBytes = 10_000) { released = true }
        val a = pool.open("a")
        pool.shutdown()
        assertTrue("a buffer is still open", !released)
        assertEquals("nothing handed over yet", 0, store.handoffs.value)
        a.close()
        assertTrue(released)
        assertEquals("a running gateway is told there may be data to pick up", 1, store.handoffs.value)
    }

    @Test
    fun `peek hands out a window in order, flash before RAM`() {
        open(ramMaxBytes = 10_000)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2))
        buffer.flushToDisk()
        buffer.enqueue("t", payload(3))
        buffer.enqueue("t", payload(4))

        assertEquals(listOf(1, 2, 3), buffer.peek(maxItems = 3).map { it.payload[0].toInt() })
        assertEquals("peeking takes nothing out", 4L, buffer.count)
        assertEquals("the payload limit ends the window early", 2, buffer.peek(maxItems = 10, maxPayloadBytes = 90).size)
        assertEquals("but never below one message", 1, buffer.peek(maxItems = 10, maxPayloadBytes = 1).size)
    }

    @Test
    fun `items of a window can be removed in any order`() {
        open(ramMaxBytes = 10_000)
        buffer.enqueue("t", payload(1))
        buffer.flushToDisk()
        buffer.enqueue("t", payload(2))
        buffer.enqueue("t", payload(3))
        val (one, two, three) = buffer.peek(maxItems = 3)

        buffer.remove(three) // acknowledged first
        assertEquals(listOf(1, 2), buffer.peek(maxItems = 3).map { it.payload[0].toInt() })
        buffer.remove(one)
        buffer.remove(two)
        assertTrue(buffer.isEmpty)
        assertEquals(0L, pool.ramBytes)
        assertEquals(0L, store.bytes)
    }

    @Test
    fun `flash is read ahead and cleared a batch at a time`() {
        open(ramMaxBytes = 10_000)
        repeat(3) { buffer.enqueue("t", payload(it + 1)) }
        buffer.flushToDisk()

        buffer.remove(buffer.takeNext()!!)
        assertEquals("the published row stays until its batch is through", 3L, store.count("dev"))
        assertEquals("but no longer counts as waiting", 2L, buffer.count)
        assertEquals(80L, buffer.diskBytes)

        buffer.remove(buffer.takeNext()!!)
        buffer.remove(buffer.takeNext()!!)
        assertEquals("the batch is deleted together", 0L, store.count("dev"))
        assertTrue(buffer.isEmpty)
    }

    @Test
    fun `RAM waits until flash is through, also beyond what is read ahead`() {
        open(ramMaxBytes = 100_000, diskMaxBytes = 100_000)
        repeat(70) { buffer.enqueue("t", payload(it + 1, size = 1)) } // more rows than one read-ahead
        buffer.flushToDisk()
        buffer.enqueue("t", payload(99, size = 1))

        val first = buffer.peek(maxItems = 100)
        assertEquals("only what was read ahead: older rows are still in flash", 64, first.size)
        first.forEach(buffer::remove)

        val rest = buffer.peek(maxItems = 100)
        assertEquals(listOf(65, 66, 67, 68, 69, 70, 99), rest.map { it.payload[0].toInt() })
    }

    @Test
    fun `closing commits the published part of a batch`() {
        open(ramMaxBytes = 10_000)
        repeat(3) { buffer.enqueue("t", payload(it + 1)) }
        buffer.flushToDisk()
        buffer.remove(buffer.takeNext()!!)

        buffer.close()

        assertEquals("only the unpublished rows are left for the next run", 2L, store.count("dev"))
        assertEquals(2L, store.peek("dev")!!.seq)
    }

    @Test
    fun `rows evicted while read ahead are not handed out again`() {
        open(ramMaxBytes = 40, diskMaxBytes = 120)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2)) // both spill to flash
        val inFlight = buffer.takeNext()!! // reads 1 and 2 ahead; 1 is being published

        buffer.enqueue("t", payload(3))
        buffer.enqueue("t", payload(4)) // spills 3 and 4: flash is over its limit and 1 is evicted

        assertEquals(listOf(2, 3, 4), buffer.peek(maxItems = 10).map { it.payload[0].toInt() })
        buffer.remove(inFlight) // its publish completes anyway: nothing left to delete
        assertEquals(listOf(2, 3, 4), buffer.drainMarkers())
    }

    @Test
    fun `a message arriving after close is dropped rather than leaking budget`() {
        open(ramMaxBytes = 10_000)
        buffer.close()

        buffer.enqueue("t", payload(1))

        assertEquals(0L, pool.ramBytes)
        assertNull(buffer.takeNext())
    }

    @Test
    fun `sizes include the overhead of holding a message`() {
        open(ramMaxBytes = 10_000, itemOverhead = 10, rowOverhead = 20)
        buffer.enqueue("t", payload(1, size = 5))
        assertEquals(15L, buffer.ramBytes)
        assertEquals(15L, pool.usage.value.ramBytes)

        buffer.flushToDisk()
        assertEquals(0L, buffer.ramBytes)
        assertEquals(25L, buffer.diskBytes)
        assertEquals(25L, pool.usage.value.flashBytes)
    }

    @Test
    fun `when flash cannot be used the pool drops instead of spilling`() {
        // A directory where the database file should be: it can't be opened.
        context.getDatabasePath("spotflow_buffer.db").mkdirs()
        open(ramMaxBytes = 100)

        repeat(4) { buffer.enqueue("t", payload(it + 1)) } // 160 bytes > 100

        assertEquals(false, pool.storeAvailable.value)
        assertTrue("RAM stays bounded", pool.ramBytes <= 100)
        assertEquals("the newest survive", listOf(3, 4), buffer.drainMarkers())
    }
}

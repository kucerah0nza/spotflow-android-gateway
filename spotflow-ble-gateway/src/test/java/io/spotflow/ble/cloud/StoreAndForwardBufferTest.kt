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

    private fun open(ramMaxBytes: Long, diskMaxBytes: Long = 10_000, spill: Boolean = true) {
        store = MessageStore(context, diskMaxBytes)
        pool = BufferPool(store, ramMaxBytes, flashMaxBytes = if (spill) diskMaxBytes else 0)
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

        val restarted = BufferPool(store, 10_000, flashMaxBytes = 10_000).open("dev")
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
        a.close()
        assertTrue(released)
    }
}

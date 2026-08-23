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
    private lateinit var disk: PersistentMessageQueue
    private lateinit var buffer: StoreAndForwardBuffer

    private fun payload(marker: Int, size: Int = 40) = ByteArray(size) { marker.toByte() }

    private fun open(ramMaxBytes: Long, diskMaxBytes: Long = 10_000) {
        disk = PersistentMessageQueue(context, "dev", diskMaxBytes)
        buffer = StoreAndForwardBuffer(disk, ramMaxBytes)
    }

    /** Drains everything in order, returning the marker of each item. */
    private fun drainMarkers(): List<Int> {
        val out = mutableListOf<Int>()
        while (true) {
            val item = buffer.takeNext() ?: break
            out += item.payload[0].toInt()
            buffer.remove(item)
        }
        return out
    }

    @Before
    fun setUp() {
        context.deleteDatabase("spotflow_buffer_dev.db")
    }

    @After
    fun tearDown() {
        if (::buffer.isInitialized) buffer.close()
    }

    @Test
    fun `stays in RAM under the cap`() {
        open(ramMaxBytes = 100)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2)) // 80 bytes, under cap

        assertEquals("disk tier must be untouched", 0L, disk.bytes)
        assertEquals(80L, buffer.bytes)
        assertEquals(listOf(1, 2), drainMarkers())
    }

    @Test
    fun `spills oldest to disk over the cap`() {
        open(ramMaxBytes = 100)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2))
        buffer.enqueue("t", payload(3)) // 120 > 100 -> spill marker 1 to disk

        assertEquals(40L, disk.bytes)
        assertEquals(120L, buffer.bytes)
        assertEquals("disk (oldest) drained first", 1, buffer.takeNext()!!.payload[0].toInt())
    }

    @Test
    fun `preserves FIFO across tiers`() {
        open(ramMaxBytes = 100) // holds ~2 items; the rest spill to disk
        repeat(6) { buffer.enqueue("t", payload(it + 1)) }
        assertTrue("some should have spilled", disk.bytes > 0)
        assertEquals(listOf(1, 2, 3, 4, 5, 6), drainMarkers())
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
    fun `remove finalizes an item spilled to disk mid-publish`() {
        open(ramMaxBytes = 40) // any second item forces the first to spill
        buffer.enqueue("t", payload(1))
        val inflight = buffer.takeNext()!! // peeked from RAM (seq 1)
        assertEquals(1, inflight.payload[0].toInt())

        // While "publishing", more data arrives and spills the in-flight item to disk.
        buffer.enqueue("t", payload(2))
        assertTrue(disk.bytes > 0)

        // Finalizing by sequence must remove it from disk (no duplicate, no reorder).
        buffer.remove(inflight)
        assertEquals(0L, disk.bytes)
        assertEquals(2, buffer.takeNext()!!.payload[0].toInt())
    }

    @Test
    fun `flushToDisk moves RAM items to the persistent tier in order`() {
        open(ramMaxBytes = 10_000)
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2))
        assertEquals(0L, disk.bytes)

        buffer.flushToDisk()

        assertTrue("data should now be on disk", disk.bytes > 0)
        assertEquals(listOf(1, 2), drainMarkers())
    }
}

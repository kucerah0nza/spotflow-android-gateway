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
        buffer.enqueue("t", payload(2)) // 80 bytes total, under cap

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

        val first = buffer.takeNext()!!
        assertEquals("disk (oldest) drained first", 1, first.payload[0].toInt())
    }

    @Test
    fun `preserves FIFO across tiers`() {
        open(ramMaxBytes = 100) // holds ~2 items; the rest spill to disk
        repeat(6) { buffer.enqueue("t", payload(it + 1)) }
        assertTrue("some should have spilled", disk.bytes > 0)
        assertEquals(listOf(1, 2, 3, 4, 5, 6), drainMarkers())
    }

    @Test
    fun `requeue returns a RAM item for retry`() {
        open(ramMaxBytes = 1000)
        buffer.enqueue("t", payload(7))

        val item = buffer.takeNext()!! // popped from RAM
        assertNull(buffer.takeNext()) // nothing left
        buffer.requeue(item)
        assertEquals(7, buffer.takeNext()!!.payload[0].toInt())
    }

    @Test
    fun `remove finalizes a disk item`() {
        open(ramMaxBytes = 40) // tiny, so the second enqueue spills the first to disk
        buffer.enqueue("t", payload(1))
        buffer.enqueue("t", payload(2))
        assertTrue(disk.bytes > 0)

        val diskItem = buffer.takeNext()!!
        assertEquals(1, diskItem.payload[0].toInt())
        buffer.requeue(diskItem) // disk item stays queued
        assertEquals(1, buffer.takeNext()!!.payload[0].toInt())
        buffer.remove(buffer.takeNext()!!) // remove marker 1 for good
        assertEquals(0L, disk.bytes)
    }
}

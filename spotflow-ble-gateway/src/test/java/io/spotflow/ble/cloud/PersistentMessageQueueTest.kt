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

    private fun open(maxBytes: Long, device: String = "dev") =
        PersistentMessageQueue(context, device, maxBytes).also { queue = it }

    @Before
    fun setUp() {
        // Start each test from a clean database.
        context.deleteDatabase("spotflow_buffer_dev.db")
    }

    @After
    fun tearDown() {
        if (::queue.isInitialized) queue.close()
    }

    @Test
    fun `fifo order and removal`() {
        open(maxBytes = 10_000)
        queue.enqueue("t", payload(1, 4))
        queue.enqueue("t", payload(2, 4))
        queue.enqueue("t", payload(3, 4))

        val first = queue.peek()!!
        assertEquals(1, first.payload[0].toInt())
        queue.remove(first.id)
        assertEquals(2, queue.peek()!!.payload[0].toInt())
    }

    @Test
    fun `bytes accounting tracks enqueue and remove`() {
        open(maxBytes = 10_000)
        queue.enqueue("t", payload(1, 100))
        queue.enqueue("t", payload(2, 50))
        assertEquals(150, queue.bytes)

        queue.remove(queue.peek()!!.id)
        assertEquals(50, queue.bytes)
    }

    @Test
    fun `eviction drops oldest to stay within cap`() {
        open(maxBytes = 100)
        queue.enqueue("t", payload(1, 40))
        queue.enqueue("t", payload(2, 40))
        queue.enqueue("t", payload(3, 40)) // 120 > 100 -> evict marker 1

        assertEquals(80, queue.bytes)
        assertEquals(2, queue.peek()!!.payload[0].toInt())

        queue.enqueue("t", payload(4, 40)) // evict marker 2
        assertEquals(80, queue.bytes)
        assertEquals(3, queue.peek()!!.payload[0].toInt())
    }

    @Test
    fun `single payload larger than cap is kept alone`() {
        open(maxBytes = 100)
        queue.enqueue("t", payload(9, 200))
        assertEquals(200, queue.bytes)
        assertEquals(9, queue.peek()!!.payload[0].toInt())
    }

    @Test
    fun `survives close and reopen`() {
        open(maxBytes = 10_000)
        queue.enqueue("cfg", payload(7, 30))
        queue.close()

        open(maxBytes = 10_000)
        val entry = queue.peek()!!
        assertEquals("cfg", entry.topic)
        assertEquals(7, entry.payload[0].toInt())
        assertEquals(30, queue.bytes)
    }

    @Test
    fun `peek on empty queue returns null`() {
        open(maxBytes = 10_000)
        assertNull(queue.peek())
        assertEquals(0, queue.bytes)
    }
}

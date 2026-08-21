package io.spotflow.ble.protocol

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.random.Random

class FrameCodecTest {

    /** Feeds every frame through a fresh reassembler and returns the single completed message. */
    private fun roundTrip(type: MessageType, payload: ByteArray, seq: Int, mtu: Int): Message {
        val frames = FrameCodec.fragment(type, payload, seq, mtu)
        val reassembler = FrameCodec.Reassembler()
        var result: Message? = null
        for ((index, frame) in frames.withIndex()) {
            val out = reassembler.onFragment(frame)
            if (index < frames.lastIndex) {
                assertNull("message completed early at fragment $index", out)
            } else {
                result = out
            }
        }
        return requireNotNull(result) { "message did not reassemble" }
    }

    @Test
    fun `single fragment round trip`() {
        val payload = "hello".toByteArray()
        val msg = roundTrip(MessageType.TELEMETRY, payload, seq = 7, mtu = 247)
        assertEquals(MessageType.TELEMETRY, msg.type)
        assertArrayEquals(payload, msg.payload)
    }

    @Test
    fun `fits in one frame at minimum mtu produces exactly one frame`() {
        // At MTU 23: att payload = 20, first-fragment capacity = 15.
        val payload = ByteArray(15) { it.toByte() }
        val frames = FrameCodec.fragment(MessageType.TELEMETRY, payload, seq = 0, mtu = 23)
        assertEquals(1, frames.size)
        val flags = frames[0][1].toInt() and 0xFF
        assertTrue(flags and FrameCodec.FLAG_IS_FIRST != 0)
        assertTrue(flags and FrameCodec.FLAG_IS_LAST != 0)
    }

    @Test
    fun `multi fragment round trip at minimum mtu`() {
        val payload = Random(42).nextBytes(500)
        val frames = FrameCodec.fragment(MessageType.REPORTED_CONFIGURATION, payload, seq = 3, mtu = 23)
        assertTrue("expected many fragments, got ${frames.size}", frames.size > 20)
        val msg = roundTrip(MessageType.REPORTED_CONFIGURATION, payload, seq = 3, mtu = 23)
        assertArrayEquals(payload, msg.payload)
    }

    @Test
    fun `first and last flags land on the right fragments`() {
        val payload = Random(1).nextBytes(200)
        val frames = FrameCodec.fragment(MessageType.TELEMETRY, payload, seq = 1, mtu = 40)
        frames.forEachIndexed { i, f ->
            val flags = f[1].toInt() and 0xFF
            assertEquals("IS_FIRST on fragment $i", i == 0, flags and FrameCodec.FLAG_IS_FIRST != 0)
            assertEquals("IS_LAST on fragment $i", i == frames.lastIndex, flags and FrameCodec.FLAG_IS_LAST != 0)
            assertEquals("seq on fragment $i", 1, f[2].toInt() and 0xFF)
        }
    }

    @Test
    fun `empty payload produces one first-and-last frame`() {
        val frames = FrameCodec.fragment(MessageType.DESIRED_CONFIGURATION, ByteArray(0), seq = 9, mtu = 100)
        assertEquals(1, frames.size)
        val msg = roundTrip(MessageType.DESIRED_CONFIGURATION, ByteArray(0), seq = 9, mtu = 100)
        assertEquals(0, msg.payload.size)
    }

    @Test
    fun `first fragment declares correct total length little endian`() {
        val payload = ByteArray(0x0102) // 258 bytes
        val frames = FrameCodec.fragment(MessageType.TELEMETRY, payload, seq = 0, mtu = 100)
        assertEquals(0x02, frames[0][3].toInt() and 0xFF) // low byte
        assertEquals(0x01, frames[0][4].toInt() and 0xFF) // high byte
    }

    @Test
    fun `max length message round trips`() {
        val payload = Random(9).nextBytes(FrameCodec.MAX_MESSAGE_LENGTH)
        val msg = roundTrip(MessageType.TELEMETRY, payload, seq = 255, mtu = 517)
        assertArrayEquals(payload, msg.payload)
    }

    @Test
    fun `continuation without first fragment is dropped`() {
        val reassembler = FrameCodec.Reassembler()
        val orphan = byteArrayOf(MessageType.TELEMETRY.value.toByte(), 0x00, 0x05, 1, 2, 3)
        assertNull(reassembler.onFragment(orphan))
    }

    @Test
    fun `unknown message type is dropped on completion`() {
        val reassembler = FrameCodec.Reassembler()
        val flags = (FrameCodec.FLAG_IS_FIRST or FrameCodec.FLAG_IS_LAST).toByte()
        val frame = byteArrayOf(0x7F, flags, 0x00, 0x01, 0x00, 42)
        assertNull(reassembler.onFragment(frame))
    }

    @Test
    fun `too short frame is dropped`() {
        val reassembler = FrameCodec.Reassembler()
        assertNull(reassembler.onFragment(byteArrayOf(0x02, 0x01)))
    }

    @Test
    fun `interleaved messages with different seq reassemble independently`() {
        val reassembler = FrameCodec.Reassembler()
        val a = Random(1).nextBytes(120)
        val b = Random(2).nextBytes(90)
        val framesA = FrameCodec.fragment(MessageType.TELEMETRY, a, seq = 10, mtu = 40)
        val framesB = FrameCodec.fragment(MessageType.TELEMETRY, b, seq = 11, mtu = 40)

        val completed = mutableListOf<Message>()
        // Interleave: one from A, one from B, repeatedly.
        val max = maxOf(framesA.size, framesB.size)
        for (i in 0 until max) {
            framesA.getOrNull(i)?.let { reassembler.onFragment(it)?.let(completed::add) }
            framesB.getOrNull(i)?.let { reassembler.onFragment(it)?.let(completed::add) }
        }
        assertEquals(2, completed.size)
        assertTrue(completed.any { it.payload.contentEquals(a) })
        assertTrue(completed.any { it.payload.contentEquals(b) })
    }

    @Test
    fun `reset drops in-flight partials`() {
        val reassembler = FrameCodec.Reassembler()
        val frames = FrameCodec.fragment(MessageType.TELEMETRY, Random(3).nextBytes(200), seq = 4, mtu = 30)
        reassembler.onFragment(frames.first())
        reassembler.reset()
        // Feeding the remaining continuations now has no first fragment to attach to.
        for (i in 1 until frames.size) {
            assertNull(reassembler.onFragment(frames[i]))
        }
    }
}

package io.spotflow.ble.protocol

import io.spotflow.ble.protocol.ArtifactProtocol.Header
import io.spotflow.ble.protocol.ArtifactProtocol.Request
import io.spotflow.ble.protocol.ArtifactProtocol.Status
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

class ArtifactProtocolTest {

    private fun le(size: Int) = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)

    private fun header(buffer: ByteBuffer, op: Int, transfer: Long = 0xA1B2C3D4, request: Long = 7): ByteBuffer =
        buffer.put(op.toByte()).putInt(transfer.toInt()).putInt(request.toInt())

    private fun open(url: String, secret: String, transfer: Long = 0xA1B2C3D4): ByteArray {
        val u = url.toByteArray()
        val s = secret.toByteArray()
        return header(le(12 + u.size + s.size), ArtifactProtocol.OP_OPEN, transfer)
            .putShort(u.size.toShort()).put(s.size.toByte()).put(u).put(s).array()
    }

    private fun read(offset: Long, count: Int) =
        header(le(15), ArtifactProtocol.OP_READ).putInt(offset.toInt()).putShort(count.toShort()).array()

    private fun close(consumed: Long, reason: Int) =
        header(le(14), ArtifactProtocol.OP_CLOSE).putInt(consumed.toInt()).put(reason.toByte()).array()

    private fun malformed(payload: ByteArray) = ArtifactProtocol.parse(payload) is Request.Malformed

    @Test
    fun `parses OPEN, with IDs as unsigned integers`() {
        val request = ArtifactProtocol.parse(open("https://example.com/a.bin", "s3cr3t")) as Request.Open
        assertEquals(Header(ArtifactProtocol.OP_OPEN, 0xA1B2C3D4, 7), request.header)
        assertEquals("https://example.com/a.bin", request.url)
        assertEquals("s3cr3t", request.secret)
    }

    @Test
    fun `an OPEN request never prints its URL or secret`() {
        val text = ArtifactProtocol.parse(open("https://example.com/a.bin", "s3cr3t")).toString()
        assertFalse(text.contains("example") || text.contains("s3cr3t"))
    }

    @Test
    fun `parses READ and CLOSE`() {
        assertEquals(Request.Read(Header(2, 0xA1B2C3D4, 7), 0xFFFF0000, 4096), ArtifactProtocol.parse(read(0xFFFF0000, 4096)))
        assertEquals(Request.Close(Header(3, 0xA1B2C3D4, 7), 512, complete = true), ArtifactProtocol.parse(close(512, 0)))
        assertEquals(Request.Close(Header(3, 0xA1B2C3D4, 7), 512, complete = false), ArtifactProtocol.parse(close(512, 1)))
    }

    @Test
    fun `a request without a whole header gets no answer`() {
        assertNull(ArtifactProtocol.parse(ByteArray(8)))
    }

    @Test
    fun `requests that break the protocol are malformed, with their header kept for the answer`() {
        assertTrue("transfer ID 0", malformed(open("https://a", "s", transfer = 0)))
        assertTrue("empty secret", malformed(open("https://a", "")))
        assertTrue("secret too long", malformed(open("https://a", "x".repeat(25))))
        assertTrue("secret with a space", malformed(open("https://a", "a b")))
        assertTrue("URL too long", malformed(open("https://" + "a".repeat(121), "s")))
        assertTrue("trailing bytes", malformed(open("https://a", "s") + 0))
        assertTrue("READ of nothing", malformed(read(0, 0)))
        assertTrue("READ over 4096", malformed(read(0, 4097)))
        assertTrue("short READ", malformed(read(0, 1).copyOf(14)))
        assertTrue("CLOSE reason 2", malformed(close(0, 2)))
        assertTrue("unknown operation", malformed(header(le(9), 0x04).array()))
        assertEquals(7L, ArtifactProtocol.parse(read(0, 0))!!.header.requestId)
    }

    @Test
    fun `responses echo the IDs with the response bit set`() {
        val header = Header(ArtifactProtocol.OP_READ, 0xA1B2C3D4, 0x01020304)
        val block = ArtifactProtocol.block(header, 0x10000, byteArrayOf(9, 8, 7))
        val expected = le(17).put(0x82.toByte()).putInt(0xA1B2C3D4.toInt()).putInt(0x01020304).put(0)
            .putInt(0x10000).put(byteArrayOf(9, 8, 7)).array()
        assertArrayEquals(expected, block)

        val opened = ArtifactProtocol.opened(header.copy(operation = ArtifactProtocol.OP_OPEN), 0xFFFFFFFF)
        assertEquals(14, opened.size)
        assertEquals(0x81, opened[0].toInt() and 0xFF)
        assertEquals(-1, ByteBuffer.wrap(opened, 10, 4).order(ByteOrder.LITTLE_ENDIAN).int)

        val error = ArtifactProtocol.status(header.copy(operation = ArtifactProtocol.OP_CLOSE), Status.TOO_LARGE)
        assertEquals(10, error.size)
        assertEquals(0x83, error[0].toInt() and 0xFF)
        assertEquals(4, error[9].toInt())
    }
}

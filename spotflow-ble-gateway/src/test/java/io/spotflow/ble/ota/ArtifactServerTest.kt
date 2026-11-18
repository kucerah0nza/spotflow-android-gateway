package io.spotflow.ble.ota

import io.spotflow.ble.OtaTransfer
import io.spotflow.ble.protocol.ArtifactProtocol
import io.spotflow.ble.protocol.ArtifactProtocol.Status
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.random.Random

/** The artifact transfer as a device drives it, against the gateway-wide cache and a fake download. */
@OptIn(ExperimentalCoroutinesApi::class)
class ArtifactServerTest {

    @get:Rule val folder = TemporaryFolder()

    private val firmware = Random(1).nextBytes(10_000)
    private val other = Random(2).nextBytes(3_000)

    /** Serves [firmware] at URL "a" and [other] at "b", secret "s"; anything else is denied. */
    private inner class FakeDownloader : ArtifactDownloader {
        val calls = mutableListOf<String>()
        /** While set, a download waits for it. */
        var gate: CompletableDeferred<Unit>? = null
        var failWith: Status? = null

        override suspend fun download(url: String, secret: String, maxBytes: Long, target: File): Long {
            calls += url
            gate?.await()
            failWith?.let { throw ArtifactDownloadException(it, "failing") }
            val bytes = when {
                secret != "s" -> throw ArtifactDownloadException(Status.ACCESS_DENIED, "HTTP 403")
                url == "a" -> firmware
                url == "b" -> other
                else -> throw ArtifactDownloadException(Status.ACCESS_DENIED, "HTTP 404")
            }
            if (bytes.size > maxBytes) throw ArtifactDownloadException(Status.TOO_LARGE, "too large")
            target.writeBytes(bytes)
            return bytes.size.toLong()
        }
    }

    private val downloader = FakeDownloader()
    private val sent = mutableListOf<ByteArray>()
    private val progress = mutableListOf<OtaTransfer?>()
    private lateinit var dir: File

    private fun TestScope.cache(maxEntries: Int = 4, maxBytes: Long = 1_000_000) =
        ArtifactCache({ dir }, downloader, maxBytes, backgroundScope, maxEntries)

    private fun server(cache: ArtifactCache) = ArtifactServer(cache, "dev", { sent += it }) { progress += it }

    private fun le(size: Int) = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)

    private fun header(size: Int, op: Int, transfer: Int, request: Int) =
        le(size).put(op.toByte()).putInt(transfer).putInt(request)

    private fun open(transfer: Int, request: Int, url: String = "a", secret: String = "s") =
        header(12 + url.length + secret.length, ArtifactProtocol.OP_OPEN, transfer, request)
            .putShort(url.length.toShort()).put(secret.length.toByte()).put(url.toByteArray()).put(secret.toByteArray()).array()

    private fun read(transfer: Int, request: Int, offset: Int, count: Int) =
        header(15, ArtifactProtocol.OP_READ, transfer, request).putInt(offset).putShort(count.toShort()).array()

    private fun close(transfer: Int, request: Int, consumed: Int) =
        header(14, ArtifactProtocol.OP_CLOSE, transfer, request).putInt(consumed).put(0).array()

    private class Response(bytes: ByteArray) {
        private val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        val op = buffer.get().toInt() and 0xFF
        val transfer = buffer.int
        val request = buffer.int
        val status = buffer.get().toInt()
        val rest = ByteArray(buffer.remaining()).also { buffer.get(it) }
        val u32 get() = ByteBuffer.wrap(rest).order(ByteOrder.LITTLE_ENDIAN).int.toLong() and 0xFFFFFFFFL
    }

    private fun lastResponse() = Response(sent.last())

    private fun setUp() {
        dir = folder.newFolder("ota")
    }

    @Test
    fun `a device opens, reads and closes an artifact`() = runTest {
        setUp()
        val server = server(cache())
        server.handle(this, open(transfer = 5, request = 1)); runCurrent()
        with(lastResponse()) {
            assertEquals(0x81, op)
            assertEquals(5 to 1, transfer to request)
            assertEquals(0, status)
            assertEquals(firmware.size.toLong(), u32)
        }

        server.handle(this, read(transfer = 5, request = 2, offset = 4096, count = 4096)); runCurrent()
        with(lastResponse()) {
            assertEquals(0x82 to 0, op to status)
            assertEquals(4096L, u32)
            assertArrayEquals(firmware.copyOfRange(4096, 8192), rest.copyOfRange(4, rest.size))
        }
        assertEquals(OtaTransfer(firmware.size.toLong(), 8192), progress.last())

        server.handle(this, close(transfer = 5, request = 3, consumed = firmware.size)); runCurrent()
        assertEquals(0x83 to 0, lastResponse().op to lastResponse().status)
        assertEquals(10, sent.last().size)
        assertEquals(null, progress.last())
    }

    @Test
    fun `an OPEN repeated while its download runs is answered once, and downloads once`() = runTest {
        setUp()
        downloader.gate = CompletableDeferred()
        val server = server(cache())
        repeat(5) { server.handle(this, open(transfer = 5, request = 1)); runCurrent() }
        assertTrue(sent.isEmpty())
        assertEquals(OtaTransfer(null, 0), progress.last())

        downloader.gate!!.complete(Unit); runCurrent()
        assertEquals(1, sent.size)
        assertEquals(1, downloader.calls.size)
    }

    @Test
    fun `a request answered already is answered again when repeated`() = runTest {
        setUp()
        val server = server(cache())
        server.handle(this, open(transfer = 5, request = 1)); runCurrent()
        server.handle(this, read(transfer = 5, request = 2, offset = 0, count = 100)); runCurrent()
        server.handle(this, read(transfer = 5, request = 2, offset = 0, count = 100)); runCurrent()

        assertEquals(3, sent.size)
        assertArrayEquals(sent[1], sent[2])
    }

    @Test
    fun `the download goes on when the device gives up, and its next transfer is served from it`() = runTest {
        setUp()
        downloader.gate = CompletableDeferred()
        val cache = cache()
        val session = CoroutineScope(coroutineContext + Job(coroutineContext[Job]))
        server(cache).handle(session, open(transfer = 5, request = 1)); runCurrent()
        session.cancel() // the BLE session ended
        downloader.gate!!.complete(Unit); runCurrent()
        assertTrue(sent.isEmpty())

        // Reconnected: a new session, a new transfer.
        val server = server(cache)
        server.handle(this, open(transfer = 6, request = 1)); runCurrent()
        assertEquals(0, lastResponse().status)
        assertEquals(1, downloader.calls.size)
    }

    @Test
    fun `devices updating to the same artifact share one download`() = runTest {
        setUp()
        downloader.gate = CompletableDeferred()
        val cache = cache()
        server(cache).handle(this, open(transfer = 5, request = 1))
        server(cache).handle(this, open(transfer = 9, request = 1))
        runCurrent()
        downloader.gate!!.complete(Unit); runCurrent()

        assertEquals(2, sent.size)
        assertEquals(1, downloader.calls.size)
    }

    @Test
    fun `a failed download is reported with its status and tried again on the next OPEN`() = runTest {
        setUp()
        val server = server(cache())
        downloader.failWith = Status.TRANSIENT_FAILURE
        server.handle(this, open(transfer = 5, request = 1)); runCurrent()
        assertEquals(3, lastResponse().status)
        assertEquals(null, progress.last())

        downloader.failWith = null
        server.handle(this, open(transfer = 5, request = 2)); runCurrent()
        assertEquals(0, lastResponse().status)
        assertEquals(2, downloader.calls.size)

        server.handle(this, open(transfer = 6, request = 1, secret = "wrong")); runCurrent()
        assertEquals(Status.ACCESS_DENIED.code, lastResponse().status)
    }

    @Test
    fun `an artifact over the limit is refused as too large`() = runTest {
        setUp()
        val server = server(cache(maxBytes = 5_000))
        server.handle(this, open(transfer = 5, request = 1)); runCurrent()
        assertEquals(Status.TOO_LARGE.code, lastResponse().status)
    }

    @Test
    fun `reads outside the artifact or of an unknown transfer are invalid`() = runTest {
        setUp()
        val server = server(cache())
        server.handle(this, read(transfer = 5, request = 1, offset = 0, count = 10)); runCurrent()
        assertEquals(1, lastResponse().status)

        server.handle(this, open(transfer = 5, request = 2)); runCurrent()
        server.handle(this, read(transfer = 5, request = 3, offset = firmware.size - 10, count = 11)); runCurrent()
        assertEquals(1, lastResponse().status)
        assertEquals(10, sent.last().size)

        server.handle(this, read(transfer = 5, request = 4, offset = firmware.size - 10, count = 10)); runCurrent()
        assertEquals(0, lastResponse().status)
    }

    @Test
    fun `a malformed request is answered as invalid, one without a header not at all`() = runTest {
        setUp()
        val server = server(cache())
        server.handle(this, read(transfer = 5, request = 1, offset = 0, count = 0)); runCurrent()
        assertEquals(0x82 to 1, lastResponse().op to lastResponse().status)

        server.handle(this, ByteArray(5)); runCurrent()
        assertEquals(1, sent.size)
    }

    @Test
    fun `CLOSE is answered even for a transfer closed already`() = runTest {
        setUp()
        val server = server(cache())
        server.handle(this, close(transfer = 5, request = 1, consumed = 0)); runCurrent()
        assertEquals(0, lastResponse().status)
    }

    @Test
    fun `the least recently opened artifact is evicted, and a transfer still on it fails transiently`() = runTest {
        setUp()
        val server = server(cache(maxEntries = 1))
        server.handle(this, open(transfer = 5, request = 1, url = "a")); runCurrent()
        server.handle(this, open(transfer = 6, request = 1, url = "b")); runCurrent()
        assertEquals(1, dir.listFiles()!!.size)

        server.handle(this, read(transfer = 5, request = 2, offset = 0, count = 10)); runCurrent()
        assertEquals(Status.TRANSIENT_FAILURE.code, lastResponse().status)
        server.handle(this, read(transfer = 6, request = 2, offset = 0, count = 10)); runCurrent()
        assertEquals(0, lastResponse().status)
    }

    @Test
    fun `files of an earlier run are deleted before the first download`() = runTest {
        setUp()
        File(dir, "artifact-0").writeBytes(ByteArray(10))
        File(dir, "stale").writeBytes(ByteArray(10))
        val server = server(cache())
        assertTrue("no disk I/O on creation", File(dir, "stale").exists())

        server.handle(this, open(transfer = 5, request = 1)); runCurrent()
        assertFalse(File(dir, "stale").exists())
        assertEquals(firmware.size.toLong(), File(dir, "artifact-0").length())
    }
}

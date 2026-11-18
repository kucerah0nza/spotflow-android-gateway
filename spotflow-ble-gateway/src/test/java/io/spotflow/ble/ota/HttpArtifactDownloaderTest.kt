package io.spotflow.ble.ota

import io.spotflow.ble.ota.LocalHttpServer.Request
import io.spotflow.ble.ota.LocalHttpServer.Response
import io.spotflow.ble.protocol.ArtifactProtocol.Status
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File
import kotlin.random.Random

class HttpArtifactDownloaderTest {

    @get:Rule val folder = TemporaryFolder()

    private val firmware = Random(3).nextBytes(100_000)
    private val server = LocalHttpServer()
    private val downloader = HttpArtifactDownloader(allowCleartext = true, connectTimeoutMs = 2_000, readTimeoutMs = 2_000)

    @After
    fun tearDown() = server.close()

    /** [firmware], from the byte the Range header asks for. */
    private fun firmwareFrom(request: Request): Response {
        val range = request.header("Range") ?: return Response(200, firmware)
        val from = range.removePrefix("bytes=").removeSuffix("-").toInt()
        return Response(
            206,
            firmware.copyOfRange(from, firmware.size),
            mapOf("Content-Range" to "bytes $from-${firmware.size - 1}/${firmware.size}"),
        )
    }

    /** Serves [firmware] to requests with the right secret. */
    private fun serveFirmware(path: String = "/fw.bin") = server.route(path) { request ->
        if (request.header("Authorization") != "OtaSecret s3cr3t") Response(403) else firmwareFrom(request)
    }

    private fun url(path: String, host: String = "127.0.0.1") = "http://$host:${server.port}$path"

    private fun download(url: String, secret: String = "s3cr3t", max: Long = 1_000_000, target: File = folder.root.resolve("a")) =
        runBlocking { downloader.download(url, secret, max, target) }

    private fun failure(block: () -> Unit): Status {
        try {
            block()
        } catch (e: ArtifactDownloadException) {
            return e.status
        }
        fail("expected the download to fail")
        error("unreachable")
    }

    @Test
    fun `downloads with the OTA secret`() {
        serveFirmware()
        val target = folder.root.resolve("a")
        assertEquals(firmware.size.toLong(), download(url("/fw.bin"), target = target))
        assertArrayEquals(firmware, target.readBytes())
        assertEquals("OtaSecret s3cr3t", server.requests.single().header("Authorization"))
    }

    @Test
    fun `follows a redirect, without the secret when it leads to another host`() {
        serveFirmware()
        server.route("/same") { Response(302, headers = mapOf("Location" to "/fw.bin")) }
        assertEquals(firmware.size.toLong(), download(url("/same")))

        server.requests.clear()
        server.route("/storage") { Response(200, firmware) }
        server.route("/elsewhere") { Response(307, headers = mapOf("Location" to url("/storage", host = "localhost"))) }
        assertEquals(firmware.size.toLong(), download(url("/elsewhere")))
        assertEquals("OtaSecret s3cr3t", server.requests[0].header("Authorization"))
        assertNull("the secret stays with the URL's own host", server.requests[1].header("Authorization"))
    }

    @Test
    fun `an endless redirect is refused`() {
        server.route("/loop") { Response(302, headers = mapOf("Location" to "/loop")) }
        assertEquals(Status.INVALID_REQUEST, failure { download(url("/loop")) })
    }

    @Test
    fun `refusals map to the status the device acts on`() {
        serveFirmware()
        server.route("/broken") { Response(503) }
        assertEquals(Status.ACCESS_DENIED, failure { download(url("/fw.bin"), secret = "wrong") })
        assertEquals(Status.ACCESS_DENIED, failure { download(url("/gone")) })
        assertEquals(Status.TRANSIENT_FAILURE, failure { download(url("/broken")) })
        assertEquals("a server error is tried a few times", 1 + 1 + 3, server.requests.size)
    }

    @Test
    fun `an artifact over the limit is too large, declared or not`() {
        serveFirmware()
        server.route("/undeclared") { Response(200, firmware, undeclaredLength = true) }
        val target = folder.root.resolve("a")
        assertEquals(Status.TOO_LARGE, failure { download(url("/fw.bin"), max = 99_999, target = target) })
        assertEquals(Status.TOO_LARGE, failure { download(url("/undeclared"), max = 99_999, target = target) })
        assertFalse(target.exists())
        assertEquals(firmware.size.toLong(), download(url("/undeclared"), max = 100_000, target = target))
    }

    @Test
    fun `a transfer cut off midway resumes where it stopped`() {
        var cut = true
        server.route("/flaky") { request ->
            if (cut) {
                cut = false
                Response(200, firmware, cut = 40_000)
            } else {
                firmwareFrom(request)
            }
        }
        val target = folder.root.resolve("a")
        assertEquals(firmware.size.toLong(), download(url("/flaky"), target = target))
        assertArrayEquals(firmware, target.readBytes())
        assertEquals(listOf(null, "bytes=40000-"), server.requests.map { it.header("Range") })
    }

    @Test
    fun `a server that ignores the range starts over`() {
        var cut = true
        server.route("/norange") {
            if (cut) {
                cut = false
                Response(200, firmware, cut = 40_000)
            } else {
                Response(200, firmware)
            }
        }
        val target = folder.root.resolve("a")
        assertEquals(firmware.size.toLong(), download(url("/norange"), target = target))
        assertArrayEquals(firmware, target.readBytes())
    }

    @Test
    fun `only HTTPS URLs are downloaded`() {
        val strict = HttpArtifactDownloader()
        serveFirmware()
        val status = failure { runBlocking { strict.download(url("/fw.bin"), "s3cr3t", 1_000_000, folder.root.resolve("a")) } }
        assertEquals(Status.INVALID_REQUEST, status)
        assertEquals(Status.INVALID_REQUEST, failure { download("not a url") })
        assertEquals(0, server.requests.size)
    }
}

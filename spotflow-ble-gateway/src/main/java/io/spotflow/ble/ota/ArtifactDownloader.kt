package io.spotflow.ble.ota

import io.spotflow.ble.protocol.ArtifactProtocol.Status
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.net.HttpURLConnection
import java.net.MalformedURLException
import java.net.URL

/** Downloads OTA artifacts on the devices' behalf. A seam so that tests need no web server. */
internal fun interface ArtifactDownloader {
    /**
     * Downloads the artifact at [url] into [target] (replacing it) and returns its size. Throws
     * [ArtifactDownloadException] with the status to report to the device.
     */
    suspend fun download(url: String, secret: String, maxBytes: Long, target: File): Long
}

/** A failed download, and what the device is told about it. */
internal class ArtifactDownloadException(val status: Status, message: String, cause: Throwable? = null) :
    IOException(message, cause)

/**
 * Downloads over HTTPS with `Authorization: OtaSecret <secret>`, as the Spotflow OTA protocol specifies.
 *
 * - Redirects are followed (up to [MAX_REDIRECTS]), HTTPS only; the secret goes only to the URL's own host,
 *   never to the host it redirects to (typically storage that authorizes by a signed URL).
 * - A transfer cut off midway resumes where it stopped with an HTTP Range request, if the server allows.
 * - Nothing logged here contains the URL or the secret.
 */
internal class HttpArtifactDownloader(
    /** Plain HTTP is refused unless this is set — for tests against a local server. */
    private val allowCleartext: Boolean = false,
    private val connectTimeoutMs: Int = 15_000,
    private val readTimeoutMs: Int = 30_000,
) : ArtifactDownloader {

    override suspend fun download(url: String, secret: String, maxBytes: Long, target: File): Long =
        runInterruptible(Dispatchers.IO) { downloadBlocking(url, secret, maxBytes, target) }

    private fun downloadBlocking(url: String, secret: String, maxBytes: Long, target: File): Long {
        val origin = parse(url)
        target.delete()
        var lastFailure: ArtifactDownloadException? = null
        repeat(MAX_ATTEMPTS) {
            try {
                return attempt(origin, secret, maxBytes, target)
            } catch (e: ArtifactDownloadException) {
                // Only a transfer the network broke off is worth another go right away.
                if (e.status != Status.TRANSIENT_FAILURE) throw e
                lastFailure = e
            }
        }
        throw lastFailure!!
    }

    /** One request (and its redirects), resuming from what [target] already holds. */
    private fun attempt(origin: URL, secret: String, maxBytes: Long, target: File): Long {
        val resumeFrom = if (target.exists()) target.length() else 0L
        var url = origin
        var redirects = 0
        while (true) {
            val connection = try {
                url.openConnection() as HttpURLConnection
            } catch (e: IOException) {
                throw ArtifactDownloadException(Status.TRANSIENT_FAILURE, "cannot open a connection", e)
            }
            try {
                connection.instanceFollowRedirects = false
                connection.connectTimeout = connectTimeoutMs
                connection.readTimeout = readTimeoutMs
                connection.requestMethod = "GET"
                if (sameOrigin(url, origin)) connection.setRequestProperty("Authorization", "OtaSecret $secret")
                if (resumeFrom > 0) connection.setRequestProperty("Range", "bytes=$resumeFrom-")
                val code = try {
                    connection.responseCode
                } catch (e: IOException) {
                    throw ArtifactDownloadException(Status.TRANSIENT_FAILURE, "no response: ${e.javaClass.simpleName}", e)
                }
                when (code) {
                    in 300..399 -> {
                        val location = connection.getHeaderField("Location")
                            ?: throw ArtifactDownloadException(Status.INVALID_REQUEST, "redirect without a location")
                        if (++redirects > MAX_REDIRECTS) {
                            throw ArtifactDownloadException(Status.INVALID_REQUEST, "too many redirects")
                        }
                        url = parse(URL(url, location).toString())
                        continue
                    }
                    HttpURLConnection.HTTP_OK -> return receive(connection, target, append = false, maxBytes)
                    HttpURLConnection.HTTP_PARTIAL -> {
                        // Only a continuation of exactly what we have is usable.
                        val range = connection.getHeaderField("Content-Range").orEmpty()
                        if (resumeFrom == 0L || !range.startsWith("bytes $resumeFrom-")) {
                            target.delete() // start over next time
                            throw ArtifactDownloadException(Status.TRANSIENT_FAILURE, "unusable partial response")
                        }
                        return receive(connection, target, append = true, maxBytes)
                    }
                    416 -> {
                        // What we have may already be all of it, or the artifact changed: start over.
                        target.delete()
                        throw ArtifactDownloadException(Status.TRANSIENT_FAILURE, "range not satisfiable")
                    }
                    else -> throw ArtifactDownloadException(statusFor(code), "HTTP $code")
                }
            } finally {
                connection.disconnect()
            }
        }
    }

    private fun receive(connection: HttpURLConnection, target: File, append: Boolean, maxBytes: Long): Long {
        val already = if (append) target.length() else 0L
        val declared = connection.contentLengthLong
        if (declared >= 0 && already + declared > maxBytes) {
            throw ArtifactDownloadException(Status.TOO_LARGE, "artifact of ${already + declared} bytes")
        }
        var total = already
        try {
            connection.inputStream.use { input ->
                FileOutputStream(target, append).use { output ->
                    val buffer = ByteArray(BUFFER_BYTES)
                    while (true) {
                        val read = input.read(buffer)
                        if (read < 0) break
                        total += read
                        if (total > maxBytes) throw ArtifactDownloadException(Status.TOO_LARGE, "artifact over $maxBytes bytes")
                        output.write(buffer, 0, read)
                    }
                }
            }
        } catch (e: ArtifactDownloadException) {
            target.delete()
            throw e
        } catch (e: IOException) {
            // Keep what arrived: the next attempt continues from there.
            throw ArtifactDownloadException(Status.TRANSIENT_FAILURE, "transfer interrupted after $total bytes", e)
        }
        if (declared >= 0 && total != already + declared) {
            throw ArtifactDownloadException(Status.TRANSIENT_FAILURE, "transfer ended after $total bytes")
        }
        if (total == 0L) throw ArtifactDownloadException(Status.INVALID_REQUEST, "empty artifact")
        return total
    }

    private fun parse(url: String): URL {
        val parsed = try {
            URL(url)
        } catch (e: MalformedURLException) {
            throw ArtifactDownloadException(Status.INVALID_REQUEST, "not a URL", e)
        }
        val secure = parsed.protocol.equals("https", ignoreCase = true)
        val cleartext = parsed.protocol.equals("http", ignoreCase = true)
        if (!secure && !(cleartext && allowCleartext)) {
            throw ArtifactDownloadException(Status.INVALID_REQUEST, "not an HTTPS URL")
        }
        return parsed
    }

    private companion object {
        const val MAX_ATTEMPTS = 3
        const val MAX_REDIRECTS = 5
        const val BUFFER_BYTES = 16 * 1024

        fun sameOrigin(a: URL, b: URL): Boolean =
            a.protocol.equals(b.protocol, ignoreCase = true) && a.host.equals(b.host, ignoreCase = true) &&
                (if (a.port == -1) a.defaultPort else a.port) == (if (b.port == -1) b.defaultPort else b.port)

        fun statusFor(code: Int): Status = when (code) {
            400 -> Status.INVALID_REQUEST
            401, 403, 404, 410 -> Status.ACCESS_DENIED
            413 -> Status.TOO_LARGE
            408, 425, 429 -> Status.TRANSIENT_FAILURE
            in 500..599 -> Status.TRANSIENT_FAILURE
            else -> Status.ACCESS_DENIED
        }
    }
}

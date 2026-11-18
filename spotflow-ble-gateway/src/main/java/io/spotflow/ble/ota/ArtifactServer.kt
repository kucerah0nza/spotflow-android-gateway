package io.spotflow.ble.ota

import io.spotflow.ble.OtaTransfer
import io.spotflow.ble.Slog
import io.spotflow.ble.protocol.ArtifactProtocol
import io.spotflow.ble.protocol.ArtifactProtocol.Header
import io.spotflow.ble.protocol.ArtifactProtocol.Request
import io.spotflow.ble.protocol.ArtifactProtocol.Status
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.io.IOException

/**
 * Answers one BLE session's artifact requests (OPEN, READ, CLOSE) from the [cache].
 *
 * The device drives the transfer and repeats a request (every 2 s in the Device SDK) until it gets its
 * response, each repetition being a new BLE message. A request that is already being handled — an OPEN
 * waiting for its download, typically — is therefore not handled again; one that was answered already is
 * answered again (the answer may have been lost). Answers to requests the device has stopped waiting for
 * are harmless: it matches responses to the request it is waiting for.
 *
 * URLs and secrets are never logged.
 */
internal class ArtifactServer(
    private val cache: ArtifactCache,
    private val deviceId: String,
    private val send: suspend (ByteArray) -> Unit,
    /** The transfer under way, for the device's status; null once it is closed. */
    private val onProgress: (OtaTransfer?) -> Unit = {},
) {
    private val lock = Any()

    /** Open transfers by ID. A device reopens with a new ID after a timeout, so old ones are capped. */
    private val transfers = object : LinkedHashMap<Long, ArtifactCache.Artifact>() {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Long, ArtifactCache.Artifact>) =
            size > MAX_TRANSFERS
    }

    /** Requests being handled right now. */
    private val inProgress = HashSet<Header>()

    /** Handles one reassembled ARTIFACT_REQUEST in [scope], answering it through [send]. */
    fun handle(scope: CoroutineScope, payload: ByteArray) {
        val request = ArtifactProtocol.parse(payload) ?: return // no header: nothing to answer to
        if (!synchronized(lock) { inProgress.add(request.header) }) return // a repetition of one under way
        scope.launch {
            try {
                val response = respond(request)
                try {
                    send(response)
                } catch (c: CancellationException) {
                    throw c
                } catch (t: Throwable) {
                    Slog.d(TAG, "artifact response for $deviceId not sent: ${t.message}")
                }
            } finally {
                synchronized(lock) { inProgress.remove(request.header) }
            }
        }
    }

    private suspend fun respond(request: Request): ByteArray {
        val header = request.header
        return when (request) {
            is Request.Malformed -> {
                Slog.w(TAG, "invalid artifact request from $deviceId: ${request.reason}")
                ArtifactProtocol.status(header, Status.INVALID_REQUEST)
            }
            is Request.Open -> open(request)
            is Request.Read -> {
                val artifact = synchronized(lock) { transfers[header.transferId] }
                    ?: return ArtifactProtocol.status(header, Status.INVALID_REQUEST)
                if (request.offset + request.count > artifact.size) {
                    return ArtifactProtocol.status(header, Status.INVALID_REQUEST)
                }
                val data = try {
                    artifact.read(request.offset, request.count)
                } catch (e: IOException) {
                    // Evicted from the cache meanwhile: the device reopens, which downloads it again.
                    Slog.w(TAG, "artifact for $deviceId no longer readable: ${e.message}")
                    synchronized(lock) { transfers.remove(header.transferId) }
                    return ArtifactProtocol.status(header, Status.TRANSIENT_FAILURE)
                }
                onProgress(OtaTransfer(artifact.size, request.offset + request.count))
                ArtifactProtocol.block(header, request.offset, data)
            }
            is Request.Close -> {
                // Answered even for a transfer already closed: the device repeats CLOSE if the answer was lost.
                val artifact = synchronized(lock) { transfers.remove(header.transferId) }
                if (artifact != null) {
                    val outcome = if (request.complete) "complete" else "interrupted at ${request.consumed} bytes"
                    Slog.i(TAG, "artifact transfer to $deviceId $outcome")
                }
                onProgress(null)
                ArtifactProtocol.status(header, Status.OK)
            }
        }
    }

    private suspend fun open(request: Request.Open): ByteArray {
        val header = request.header
        onProgress(OtaTransfer(sizeBytes = null, deliveredBytes = 0))
        val artifact = try {
            cache.open(request.url, request.secret)
        } catch (e: ArtifactDownloadException) {
            onProgress(null)
            return ArtifactProtocol.status(header, e.status)
        }
        if (artifact.size > MAX_ARTIFACT_SIZE) {
            onProgress(null)
            return ArtifactProtocol.status(header, Status.TOO_LARGE)
        }
        synchronized(lock) { transfers[header.transferId] = artifact }
        Slog.i(TAG, "artifact transfer to $deviceId opened (${artifact.size} bytes)")
        onProgress(OtaTransfer(artifact.size, 0))
        return ArtifactProtocol.opened(header, artifact.size)
    }

    private companion object {
        const val TAG = "SpotflowOta"
        const val MAX_TRANSFERS = 4

        /** The protocol carries sizes and offsets as uint32. */
        const val MAX_ARTIFACT_SIZE = 0xFFFFFFFFL
    }
}

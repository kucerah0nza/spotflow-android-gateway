package io.spotflow.ble.ota

import io.spotflow.ble.Slog
import io.spotflow.ble.protocol.ArtifactProtocol.Status
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.security.MessageDigest
import kotlin.concurrent.thread

/**
 * The OTA artifacts the gateway has downloaded for its devices, as files in a directory of their own.
 *
 * A device reads its artifact in small blocks over many minutes, gives up on a request after a while
 * (120 s in the Device SDK), reconnects, and resumes with a new transfer — and the protocol requires the
 * same bytes every time. So an artifact is downloaded once, completely, and served from here:
 * - a download runs in the gateway's [scope], not in the request that started it, so it carries on when the
 *   device times out or disconnects, and a later OPEN finds it done (or joins it);
 * - several devices updating to the same artifact share one download;
 * - a failed download is not remembered: the next OPEN tries again.
 *
 * At most [maxEntries] artifacts are kept; the least recently opened go first. [dir] is emptied before the
 * first download and when the cache is [cleared][clear]: artifacts are not kept across gateway runs. No disk
 * I/O happens on the caller's thread.
 */
internal class ArtifactCache(
    /** The directory, resolved on first use off the caller's thread (`Context.getCacheDir` does disk I/O). */
    directory: () -> File,
    private val downloader: ArtifactDownloader,
    /** Largest artifact accepted; a larger one is refused with [Status.TOO_LARGE]. */
    val maxArtifactBytes: Long,
    /** Runs the downloads, and the disk I/O: its dispatcher must allow blocking (the gateway's is IO). */
    scope: CoroutineScope,
    private val maxEntries: Int = 4,
) {
    /** A downloaded artifact. */
    class Artifact(val file: File, val size: Long) {
        /** Reads [count] bytes at [offset]; throws [IOException] if the file is gone (evicted). */
        fun read(offset: Long, count: Int): ByteArray = RandomAccessFile(file, "r").use { raf ->
            ByteArray(count).also {
                raf.seek(offset)
                raf.readFully(it)
            }
        }
    }

    private class Entry(val download: Deferred<Artifact>, var lastUsed: Long)

    private val dir by lazy(directory)

    // A failed download fails its Deferred only, never the scope it runs in.
    private val scope = CoroutineScope(scope.coroutineContext + SupervisorJob(scope.coroutineContext[Job]))
    private val lock = Any()
    private val entries = LinkedHashMap<String, Entry>()
    private var clock = 0L
    private var nextFile = 0L

    /** Deletes what an earlier run left in [dir]; every download waits for it, on first use. */
    private val prepared = scope.async(start = CoroutineStart.LAZY) {
        wipe()
        dir.mkdirs()
    }

    /**
     * Returns the artifact at [url], downloading it unless it is here already or on its way. Throws
     * [ArtifactDownloadException] with the status for the device.
     */
    suspend fun open(url: String, secret: String): Artifact {
        val key = keyOf(url, secret)
        val entry = synchronized(lock) {
            val existing = entries[key]?.takeUnless { it.download.isFailed() }
            (existing ?: Entry(startDownload(key, url, secret), 0).also { entries[key] = it })
                .also { it.lastUsed = ++clock }
        }
        return try {
            entry.download.await()
        } catch (c: CancellationException) {
            // Our caller gave up (its session ended); the download itself goes on for the next OPEN.
            if (entry.download.isCancelled) throw ArtifactDownloadException(Status.TRANSIENT_FAILURE, "download canceled")
            throw c
        } catch (e: ArtifactDownloadException) {
            throw e
        } catch (t: Throwable) {
            throw ArtifactDownloadException(Status.TRANSIENT_FAILURE, "download failed: ${t.javaClass.simpleName}", t)
        }
    }

    /** Stops every download and deletes every artifact, in the background. */
    fun clear() {
        synchronized(lock) {
            entries.values.forEach { it.download.cancel() }
            entries.clear()
        }
        thread(name = "spotflow-ota-clear", isDaemon = true) { runCatching { wipe() } }
    }

    /** Must hold [lock]. */
    private fun startDownload(key: String, url: String, secret: String): Deferred<Artifact> {
        val name = "artifact-${nextFile++}"
        return scope.async {
            prepared.await()
            val file = File(dir, name)
            val started = System.currentTimeMillis()
            try {
                val size = downloader.download(url, secret, maxArtifactBytes, file)
                Slog.i(TAG, "OTA artifact downloaded: $size bytes in ${System.currentTimeMillis() - started} ms")
                evictOverflow(key)
                Artifact(file, size)
            } catch (t: Throwable) {
                file.delete() // what a failed download left; resumed downloads stay within one attempt
                if (t !is CancellationException) Slog.w(TAG, "OTA artifact download failed: ${t.message}")
                throw t
            }
        }
    }

    /**
     * Makes room for the artifact [arriving] (whose download is about to complete): keeps at most
     * [maxEntries] downloaded artifacts, dropping the least recently opened.
     */
    private fun evictOverflow(arriving: String) {
        val evicted = synchronized(lock) {
            val done = entries.entries.filter { it.key != arriving && it.value.download.getCompletedOrNull() != null }
            done.sortedBy { it.value.lastUsed }.take((done.size + 1 - maxEntries).coerceAtLeast(0))
                .map { (key, entry) -> entries.remove(key); entry }
        }
        evicted.forEach { entry -> entry.download.getCompletedOrNull()?.file?.delete() }
    }

    private fun wipe() {
        dir.listFiles()?.forEach { it.delete() }
    }

    private companion object {
        const val TAG = "SpotflowOta"

        /** Identifies an artifact without keeping its URL or secret around in the clear. */
        fun keyOf(url: String, secret: String): String =
            MessageDigest.getInstance("SHA-256").digest("$url\n$secret".toByteArray())
                .joinToString("") { "%02x".format(it) }

        @OptIn(ExperimentalCoroutinesApi::class)
        fun Deferred<*>.isFailed(): Boolean = isCompleted && getCompletionExceptionOrNull() != null

        @OptIn(ExperimentalCoroutinesApi::class)
        fun <T> Deferred<T>.getCompletedOrNull(): T? = if (isCompleted && !isFailed()) getCompleted() else null
    }
}

package io.spotflow.ble

import android.util.Log
import io.spotflow.ble.cloud.MqttAuthException
import io.spotflow.ble.cloud.StoreAndForwardBuffer
import io.spotflow.ble.cloud.Uplink
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

/** A status update applied to a device's [GatewayDeviceState]. */
internal typealias StatusPush = ((GatewayDeviceState) -> GatewayDeviceState) -> Unit

/**
 * The cloud side of one device: its store-and-forward [buffer], its [uplink], and the drain loop that
 * publishes the buffer to MQTT while owning the connect/reconnect lifecycle.
 *
 * It is deliberately separate from the BLE session: when the device disconnects, the link keeps
 * uploading whatever is still buffered (see [CloudLinkRegistry]), and when the device comes back the
 * same link — still connected — is handed to the new session. Buffered data therefore reaches the cloud
 * whenever the phone is online, not only while that particular device happens to be connected.
 */
internal class CloudLink(
    val deviceId: String,
    private val buffer: StoreAndForwardBuffer,
    private val uplink: Uplink,
    @Volatile var push: StatusPush,
) {
    private val drainSignal = Channel<Unit>(Channel.CONFLATED)

    /** Set once the broker rejected the credentials; the link is then closed rather than kept alive. */
    @Volatile var authRejected = false
        private set

    private class PendingDesired(val payload: ByteArray, val ack: () -> Unit, var failures: Int = 0)

    // Desired configuration waits here (in arrival order, unacknowledged) until a BLE session writes it.
    private val desiredLock = Any()
    private val pendingDesired = ArrayDeque<PendingDesired>()
    private val desiredSignal = Channel<Unit>(Channel.CONFLATED)

    init {
        uplink.desiredConfigurationHandler = { payload, ack ->
            synchronized(desiredLock) { pendingDesired.addLast(PendingDesired(payload, ack)) }
            desiredSignal.trySend(Unit)
        }
    }

    val isEmpty: Boolean get() = buffer.isEmpty

    /** Buffers a message for upload (RAM first; never blocks on the network). */
    fun enqueue(topic: String, payload: ByteArray) {
        buffer.enqueue(topic, payload)
        pushBuffer()
        drainSignal.trySend(Unit)
    }

    /**
     * Publishes the buffer to MQTT, (re)connecting as needed. Runs until cancelled or, with
     * [stopWhenEmpty], until the buffer is empty. Throws [MqttAuthException] if the key is rejected.
     */
    suspend fun drain(stopWhenEmpty: Boolean) {
        var backoff = INITIAL_BACKOFF_MS
        var failingSeq = -1L
        var failures = 0
        while (true) {
            if (!uplink.isConnected) {
                try {
                    uplink.connect()
                    push { it.copy(cloudConnected = true, error = null) }
                    backoff = INITIAL_BACKOFF_MS
                } catch (c: CancellationException) {
                    throw c
                } catch (auth: MqttAuthException) {
                    authRejected = true
                    push { it.copy(cloudConnected = false, error = auth.message) }
                    throw auth // non-retryable: the caller stops the device
                } catch (t: Throwable) {
                    push { it.copy(cloudConnected = false, error = t.message) }
                    delay(backoff)
                    backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
                    continue
                }
            }

            val item = buffer.takeNext()
            if (item == null) {
                if (stopWhenEmpty) return
                // Idle: wait for new data, but wake periodically to keep the MQTT link warm — so a
                // drop during a quiet period is reconnected proactively instead of on the next message.
                withTimeoutOrNull(IDLE_POLL_MS) { drainSignal.receive() }
                continue
            }
            if (item.seq != failingSeq) {
                failingSeq = item.seq
                failures = 0
            }

            try {
                val published = withTimeoutOrNull(PUBLISH_TIMEOUT_MS) {
                    uplink.publish(item.topic, item.payload)
                    true
                } != null

                if (!published) {
                    // The publish stalled although the client still reports connected — typically a
                    // half-open connection on a flaky network. Force a reconnect so a hung publish can't
                    // block the buffer and make it fill up while the link looks connected.
                    Log.w(TAG, "publish stalled >${PUBLISH_TIMEOUT_MS}ms; forcing reconnect")
                    runCatching { uplink.disconnect() }
                    push { it.copy(cloudConnected = false) }
                    continue
                }

                buffer.remove(item)
                backoff = INITIAL_BACKOFF_MS
                push { it.copy(forwarded = it.forwarded + 1) }
                pushBuffer()
            } catch (c: CancellationException) {
                throw c // the item stays at the head of the buffer (takeNext only peeks)
            } catch (t: Throwable) {
                // A message that keeps failing is a poison message (e.g. too large or not authorized).
                // If it fails while the link stays up, drop it after a few tries. If the broker drops the
                // connection each time we send it, allow more tries (it could be a flaky network) but
                // still drop it eventually so it cannot block the whole buffer forever.
                val connected = uplink.isConnected
                failures++
                val limit = if (connected) MAX_PUBLISH_ATTEMPTS else MAX_PUBLISH_ATTEMPTS_WITH_DISCONNECT
                if (failures >= limit) {
                    Log.w(TAG, "dropping message after $failures failures: ${t.message}")
                    buffer.remove(item)
                    failures = 0
                }
                if (!connected) push { it.copy(cloudConnected = false) }
                pushBuffer()
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
            }
        }
    }

    /**
     * Writes pending desired configuration to the device via [write], in arrival order, acknowledging
     * each to the broker only once written. Runs until cancelled; whatever was not written stays pending
     * for the next BLE session.
     */
    suspend fun deliverDesiredConfiguration(write: suspend (ByteArray) -> Unit) {
        while (true) {
            val head = synchronized(desiredLock) { pendingDesired.firstOrNull() }
            if (head == null) {
                desiredSignal.receive()
                continue
            }
            val done = try {
                write(head.payload)
                true
            } catch (c: CancellationException) {
                throw c
            } catch (e: IllegalArgumentException) {
                Log.w(TAG, "desired configuration can never be delivered, dropping: ${e.message}")
                true
            } catch (t: Throwable) {
                Log.w(TAG, "desired configuration write failed: ${t.message}")
                if (++head.failures >= MAX_DESIRED_ATTEMPTS) {
                    Log.w(TAG, "dropping desired configuration after ${head.failures} failures")
                    true
                } else {
                    delay(DESIRED_RETRY_MS)
                    false
                }
            }
            if (done) {
                synchronized(desiredLock) { pendingDesired.removeFirst() }
                head.ack()
            }
        }
    }

    /**
     * Persists anything still in RAM, disconnects, and closes the buffer (deleting its file if empty).
     * Flushing comes first so a process kill during the (network-bound) disconnect loses nothing.
     */
    suspend fun close() = withContext(NonCancellable) {
        runCatching { buffer.flushToDisk() }
        pushBuffer()
        uplink.desiredConfigurationHandler = null
        runCatching { uplink.disconnect() }
        push { it.copy(cloudConnected = false) }
        runCatching { buffer.close() }
    }

    private fun pushBuffer() {
        val ram = buffer.ramBytes
        val disk = buffer.diskBytes
        push { it.copy(ramBytes = ram, diskBytes = disk) }
    }

    private companion object {
        const val TAG = "SpotflowGateway"
        const val INITIAL_BACKOFF_MS = 1_000L
        const val MAX_BACKOFF_MS = 30_000L
        const val MAX_PUBLISH_ATTEMPTS = 5
        const val MAX_PUBLISH_ATTEMPTS_WITH_DISCONNECT = 10
        const val IDLE_POLL_MS = 15_000L
        const val PUBLISH_TIMEOUT_MS = 20_000L
        const val MAX_DESIRED_ATTEMPTS = 3
        const val DESIRED_RETRY_MS = 2_000L
    }
}

/** Thrown when a second BLE session reports a device ID that already has a live session. */
class DuplicateDeviceException(deviceId: String) :
    IllegalStateException("device ID '$deviceId' is already connected via another Bluetooth address")

/**
 * Tracks the [CloudLink] of every device ID so there is at most one per device (one MQTT client ID, one
 * buffer file). A link is either *claimed* by a live BLE session, or *lingering*: uploading what is
 * left after its session ended, then closing itself once the buffer is empty.
 */
internal class CloudLinkRegistry(
    private val scope: CoroutineScope,
    private val createLink: (deviceId: String, push: StatusPush) -> CloudLink,
) {
    private class Lingering(val link: CloudLink) {
        lateinit var job: Job
        var handedOver = false
    }

    private val lock = Any()
    private val claimed = HashMap<String, CloudLink>()
    private val lingering = HashMap<String, Lingering>()

    /**
     * Claims the link for [deviceId] for a live session: reuses a lingering one (still connected, same
     * buffer) or creates a new one. Throws [DuplicateDeviceException] if another session holds it.
     */
    suspend fun claim(deviceId: String, push: StatusPush): CloudLink {
        val handover = synchronized(lock) {
            if (deviceId in claimed) throw DuplicateDeviceException(deviceId)
            lingering.remove(deviceId)?.also { it.handedOver = true }
        }
        try {
            handover?.job?.cancelAndJoin() // stops its drain loop; handedOver keeps the link open
            val link = handover?.link?.also { it.push = push } ?: createLink(deviceId, push)
            synchronized(lock) { claimed[deviceId] = link }
            return link
        } catch (t: Throwable) {
            handover?.link?.close()
            throw t
        }
    }

    /**
     * Ends a session's claim. Unless the key was rejected, the link lingers and keeps uploading until its
     * buffer is empty, then closes. Must be called from a non-cancellable context.
     */
    suspend fun release(link: CloudLink) {
        val lingerJob = synchronized(lock) {
            claimed.remove(link.deviceId)
            if (link.authRejected || !scope.isActive) null else startLinger(link)
        }
        if (lingerJob == null) link.close()
    }

    /** Starts draining a buffer left on disk by an earlier run, unless the device already has a link. */
    fun recover(deviceId: String) {
        synchronized(lock) {
            if (deviceId in claimed || deviceId in lingering || !scope.isActive) return
            startLinger(createLink(deviceId) {})
        }
    }

    /** Must hold [lock]. */
    private fun startLinger(link: CloudLink): Job {
        val entry = Lingering(link)
        lingering[link.deviceId] = entry
        // ATOMIC: the body (and so its finally, which closes the link) runs even if the scope is cancelled
        // right after launch — otherwise buffered RAM data could be lost without being flushed.
        entry.job = scope.launch(start = CoroutineStart.ATOMIC) {
            try {
                if (!link.isEmpty) link.drain(stopWhenEmpty = true)
                // Stay connected briefly: a device that just blipped usually reconnects within seconds,
                // and can then take over this live link instead of paying for a new TLS/MQTT handshake.
                delay(LINGER_GRACE_MS)
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                Log.w("SpotflowGateway", "background upload for ${link.deviceId} stopped: ${t.message}")
            } finally {
                val close = synchronized(lock) {
                    if (lingering[link.deviceId] === entry) lingering.remove(link.deviceId)
                    !entry.handedOver
                }
                if (close) link.close()
            }
        }
        return entry.job
    }

    private companion object {
        const val LINGER_GRACE_MS = 30_000L
    }
}

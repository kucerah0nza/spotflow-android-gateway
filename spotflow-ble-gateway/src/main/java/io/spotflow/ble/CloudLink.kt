package io.spotflow.ble

import io.spotflow.ble.cloud.MqttAuthException
import io.spotflow.ble.cloud.StoreAndForwardBuffer
import io.spotflow.ble.cloud.Uplink
import io.spotflow.ble.cloud.UplinkDisconnectedException
import io.spotflow.ble.cloud.UplinkThrottledException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
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
    initialPush: StatusPush,
    /** Whether the phone has a network: without one, connecting waits for it instead of retrying. */
    private val online: StateFlow<Boolean> = MutableStateFlow(true),
) {
    /**
     * Where status updates go: the current BLE session's status (or a no-op for a recovered link).
     * Replacing it — a new session taking over this link — immediately publishes the link's current
     * state, since a link that is already connected won't report "connected" again on its own.
     */
    @Volatile var push: StatusPush = initialPush
        set(value) {
            field = value
            publishSnapshot()
        }

    private val drainSignal = Channel<Unit>(Channel.CONFLATED)

    /** Set once the broker rejected the credentials; the link is then closed rather than kept alive. */
    @Volatile var authRejected = false
        private set

    private class PendingDesired(val payload: ByteArray, ack: () -> Unit) {
        /** One per delivery by the broker: it sends an unacknowledged message again on every reconnect. */
        val acks = arrayListOf(ack)
        var failures = 0
    }

    // Desired configuration waits here (in arrival order, unacknowledged) until a BLE session writes it.
    private val desiredLock = Any()
    private val pendingDesired = ArrayDeque<PendingDesired>()
    private val desiredSignal = Channel<Unit>(Channel.CONFLATED)

    /**
     * The last desired configuration written to the device. The broker sends the current desired
     * configuration only when an MQTT connection is established — a device on MQTT directly gets it again
     * after every reboot, since it reconnects. This link outlives BLE sessions, so a rebooted device
     * (which starts from its compiled defaults) would otherwise never get it back. Guarded by [desiredLock].
     */
    private var lastWrittenDesired: ByteArray? = null

    init {
        // Another device's data can push this device's out of RAM (to flash) or out of flash (evicted).
        buffer.onExternalChange = { pushBuffer() }
        uplink.desiredConfigurationHandler = { payload, ack ->
            // The same configuration again — what the broker does on every reconnect while a message is
            // unacknowledged — joins the one already waiting instead of queueing up behind it.
            val queued = synchronized(desiredLock) {
                val last = pendingDesired.lastOrNull()
                if (last != null && last.payload.contentEquals(payload)) {
                    last.acks += ack
                    null
                } else {
                    pendingDesired.addLast(PendingDesired(payload, ack))
                    pendingDesired.size
                }
            }
            if (queued != null) {
                Slog.d(TAG, "desired configuration for $deviceId received (${payload.size} bytes, $queued pending)")
                push { it.copy(received = it.received + 1) }
            }
            desiredSignal.trySend(Unit)
        }
        // Report a lost connection as it happens and wake an idle drain loop to reconnect, rather than
        // leaving the status "connected" until the next message or poll.
        uplink.onConnectionLost = {
            push { it.copy(cloudConnected = false) }
            drainSignal.trySend(Unit)
        }
    }

    val isEmpty: Boolean get() = buffer.isEmpty

    /** Buffers a message for upload (RAM first; never blocks on the network). */
    fun enqueue(topic: String, payload: ByteArray) {
        buffer.enqueue(topic, payload)
        pushBuffer()
        drainSignal.trySend(Unit)
    }

    /** What became of one publish. */
    private sealed interface Outcome {
        data object Delivered : Outcome

        /** No acknowledgement in time although the connection looked alive. */
        data object Stalled : Outcome

        /** The connection was lost underneath it: the network's doing, not the message's. */
        data object ConnectionLost : Outcome

        /** The broker closed the connection in response. */
        data object RejectedByDisconnect : Outcome

        /** The broker is over quota: about how much is sent, not about this message. */
        data object Throttled : Outcome

        /** The broker answered and refused the message. */
        class Refused(val reason: Throwable) : Outcome
    }

    /**
     * Publishes the buffer to MQTT, (re)connecting as needed. Runs until cancelled or, with
     * [stopWhenEmpty], until the buffer is empty. Throws [MqttAuthException] if the key is rejected.
     *
     * Up to a *window* of messages are in flight at once, sent in order, so throughput is not bound to
     * one round trip per message. Any failure shrinks the window to a single message, where the outcome
     * can be pinned on that message, and it grows back as publishes succeed.
     */
    suspend fun drain(stopWhenEmpty: Boolean) {
        var backoff = INITIAL_BACKOFF_MS
        var window = MAX_WINDOW
        var failingSeq = -1L
        var failures = 0
        while (true) {
            if (!uplink.isConnected) {
                if (!online.value) {
                    // No network: there is nothing to retry against. Wait for one.
                    push { it.copy(cloudConnected = false) }
                    online.first { it }
                    backoff = INITIAL_BACKOFF_MS
                }
                try {
                    uplink.connect()
                    push { it.copy(cloudConnected = true, error = it.error?.takeUnless { error -> isCloudError(error) }) }
                    backoff = INITIAL_BACKOFF_MS
                } catch (c: CancellationException) {
                    throw c
                } catch (auth: MqttAuthException) {
                    authRejected = true
                    push { it.copy(cloudConnected = false, error = DeviceError(DeviceError.Kind.AUTH_REJECTED, auth.describe())) }
                    throw auth // the caller decides when to try the credentials again
                } catch (t: Throwable) {
                    push { it.copy(cloudConnected = false, error = DeviceError(DeviceError.Kind.CLOUD_UNREACHABLE, t.describe())) }
                    awaitRetry(backoff)
                    backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
                    continue
                }
            }

            val batch = buffer.peek(window, MAX_WINDOW_PAYLOAD_BYTES)
            if (batch.isEmpty()) {
                if (stopWhenEmpty) return
                // Idle: wait for new data or a lost connection, and wake periodically all the same so a
                // connection that died silently is still re-established during a quiet period.
                withTimeoutOrNull(IDLE_POLL_MS) { drainSignal.receive() }
                continue
            }

            val outcomes = publishWindow(batch)

            var delivered = 0L
            batch.forEachIndexed { index, item ->
                if (outcomes[index] == Outcome.Delivered) {
                    buffer.remove(item)
                    delivered++
                }
            }
            if (delivered > 0) push { it.copy(forwarded = it.forwarded + delivered) }
            pushBuffer()
            if (delivered == batch.size.toLong()) {
                backoff = INITIAL_BACKOFF_MS
                window = (window * 2).coerceAtMost(MAX_WINDOW)
                continue
            }

            if (batch.size > 1) {
                // Several messages were in flight, so the failure can't be pinned on one of them (a
                // broker that hangs up takes them all down). Go one at a time until one gets through.
                window = 1
            } else {
                val item = batch[0]
                if (item.seq != failingSeq) {
                    failingSeq = item.seq
                    failures = 0
                }
                // A message that keeps failing on its own is a poison message (e.g. too large or not
                // authorized). Drop it after a few refusals — or after more tries if the broker hangs up
                // or stops answering instead of refusing — so it cannot block the buffer forever. A
                // connection lost to the network, or a broker over its quota, says nothing about the
                // message and is never counted.
                val limit = when (val outcome = outcomes[0]) {
                    is Outcome.Refused -> {
                        Slog.w(TAG, "publish for $deviceId refused: ${outcome.reason.message}")
                        MAX_PUBLISH_ATTEMPTS
                    }
                    Outcome.RejectedByDisconnect, Outcome.Stalled -> MAX_PUBLISH_ATTEMPTS_WITH_DISCONNECT
                    else -> null
                }
                if (limit != null && ++failures >= limit) {
                    Slog.w(TAG, "dropping a message of $deviceId after $failures failed attempts")
                    buffer.remove(item)
                    failures = 0
                    pushBuffer()
                }
            }
            if (Outcome.Stalled in outcomes) {
                // A publish stalled although the client still reports connected — typically a half-open
                // connection on a flaky network. Force a reconnect so it can't hold up the buffer.
                Slog.w(TAG, "publish for $deviceId stalled >${PUBLISH_TIMEOUT_MS}ms; forcing reconnect")
                runCatching { uplink.disconnect() }
            }
            if (!uplink.isConnected) push { it.copy(cloudConnected = false) }
            awaitRetry(backoff)
            backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
        }
    }

    /**
     * Publishes [batch] together and returns what became of each message.
     *
     * A stall is judged on the window as a whole: as long as acknowledgements keep coming, a slow uplink
     * working through a large window is healthy. Only when nothing at all is acknowledged for
     * [PUBLISH_TIMEOUT_MS] are the publishes still pending given up as stalled.
     */
    private suspend fun publishWindow(batch: List<StoreAndForwardBuffer.Item>): List<Outcome> {
        val outcomes = arrayOfNulls<Outcome>(batch.size)
        val settled = MutableStateFlow(0)
        coroutineScope {
            // UNDISPATCHED: each publish is handed to the client before the next one starts, which is what
            // keeps them in order on the wire. Cancellation leaves every item in the buffer (peek only).
            val publishes = batch.mapIndexed { index, item ->
                launch(start = CoroutineStart.UNDISPATCHED) {
                    outcomes[index] = publish(item)
                    settled.update { it + 1 }
                }
            }
            var seen = 0
            while (seen < batch.size) {
                val now = withTimeoutOrNull(PUBLISH_TIMEOUT_MS) { settled.first { it > seen } }
                if (now == null) {
                    publishes.forEach { it.cancel() }
                    break
                }
                seen = now
            }
        }
        return outcomes.map { it ?: Outcome.Stalled }
    }

    private suspend fun publish(item: StoreAndForwardBuffer.Item): Outcome =
        try {
            uplink.publish(item.topic, item.payload)
            Outcome.Delivered
        } catch (c: CancellationException) {
            throw c
        } catch (lost: UplinkDisconnectedException) {
            if (lost.rejectedByBroker) Outcome.RejectedByDisconnect else Outcome.ConnectionLost
        } catch (busy: UplinkThrottledException) {
            Outcome.Throttled
        } catch (t: Throwable) {
            Outcome.Refused(t)
        }

    /**
     * Waits out [backoff] before the next attempt — but if the network goes away and comes back in the
     * meantime, the attempt is made right away rather than at the end of a long backoff.
     */
    private suspend fun awaitRetry(backoff: Long) {
        withTimeoutOrNull(backoff) {
            online.first { !it }
            online.first { it }
        }
    }

    /**
     * Writes pending desired configuration to the device via [write], in arrival order, acknowledging
     * each to the broker only once written. Runs until cancelled; whatever was not written stays pending
     * for the next BLE session.
     *
     * A session starts by replaying the last configuration written to the device (unless newer ones are
     * pending), since the device may have rebooted and lost it — see [lastWrittenDesired].
     */
    suspend fun deliverDesiredConfiguration(write: suspend (ByteArray) -> Unit) {
        val replay = synchronized(desiredLock) { lastWrittenDesired.takeIf { pendingDesired.isEmpty() } }
        if (replay != null) {
            try {
                write(replay)
                Slog.d(TAG, "desired configuration re-sent to $deviceId (${replay.size} bytes)")
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                Slog.w(TAG, "desired configuration re-send failed: ${t.message}")
            }
        }
        while (true) {
            val head = synchronized(desiredLock) { pendingDesired.firstOrNull() }
            if (head == null) {
                desiredSignal.receive()
                continue
            }
            val done = try {
                write(head.payload)
                synchronized(desiredLock) { lastWrittenDesired = head.payload }
                Slog.d(TAG, "desired configuration written to $deviceId (${head.payload.size} bytes)")
                true
            } catch (c: CancellationException) {
                throw c
            } catch (e: IllegalArgumentException) {
                Slog.w(TAG, "desired configuration can never be delivered, dropping: ${e.message}")
                true
            } catch (t: Throwable) {
                Slog.w(TAG, "desired configuration write failed: ${t.message}")
                if (++head.failures >= MAX_DESIRED_ATTEMPTS) {
                    Slog.w(TAG, "dropping desired configuration after ${head.failures} failures")
                    true
                } else {
                    delay(DESIRED_RETRY_MS)
                    false
                }
            }
            if (done) {
                // Taken out under the lock, so no further delivery can join it after its acks are read.
                val acks = synchronized(desiredLock) {
                    pendingDesired.removeFirst()
                    head.acks.toList()
                }
                acks.forEach { it() }
            }
        }
    }

    /**
     * Persists anything still in RAM, disconnects, and closes the buffer. Flushing comes first so a
     * process kill during the (network-bound) disconnect loses nothing.
     */
    suspend fun close() = withContext(NonCancellable) {
        runCatching { buffer.flushToDisk() }
        pushBuffer()
        uplink.desiredConfigurationHandler = null
        uplink.onConnectionLost = null
        buffer.onExternalChange = null
        runCatching { uplink.disconnect() }
        push { it.copy(cloudConnected = false) }
        runCatching { buffer.close() }
    }

    private fun publishSnapshot() {
        val connected = uplink.isConnected
        val pending = buffer.count
        val ram = buffer.ramBytes
        val disk = buffer.diskBytes
        push {
            it.copy(
                cloudConnected = connected,
                error = if (connected) it.error?.takeUnless { error -> isCloudError(error) } else it.error,
                pendingMessages = pending,
                ramBytes = ram,
                diskBytes = disk,
            )
        }
    }

    private fun pushBuffer() {
        val pending = buffer.count
        val ram = buffer.ramBytes
        val disk = buffer.diskBytes
        push { it.copy(pendingMessages = pending, ramBytes = ram, diskBytes = disk) }
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

        /** Most messages in flight at once, and the most payload they may add up to. */
        const val MAX_WINDOW = 16
        const val MAX_WINDOW_PAYLOAD_BYTES = 256L * 1024

        /** Errors that a working cloud connection disproves (as opposed to Bluetooth-side ones). */
        fun isCloudError(error: DeviceError): Boolean =
            error.kind == DeviceError.Kind.CLOUD_UNREACHABLE || error.kind == DeviceError.Kind.AUTH_REJECTED

        fun Throwable.describe(): String = message ?: javaClass.simpleName
    }
}

/**
 * Tracks the [CloudLink] of every device ID so there is at most one per device (one MQTT client ID, one
 * queue in the buffer). A link is either *claimed* by a live BLE session, or *lingering*: uploading what
 * is left after its session ended, then closing itself once the buffer is empty.
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

    /** Device IDs whose claim is under way (waiting for a lingering link to stop, or creating one). */
    private val claiming = HashSet<String>()
    private val lingering = HashMap<String, Lingering>()

    /**
     * Claims the link for [deviceId] for a live session: reuses a lingering one (still connected, same
     * buffer) or creates a new one. Throws [DuplicateDeviceException] if another session holds it — or is
     * just claiming it: the ID is reserved before the hand-over starts, so two sessions can't both win.
     */
    suspend fun claim(deviceId: String, push: StatusPush): CloudLink {
        val handover = synchronized(lock) {
            if (deviceId in claimed || !claiming.add(deviceId)) throw DuplicateDeviceException(deviceId)
            lingering.remove(deviceId)?.also { it.handedOver = true }
        }
        try {
            handover?.job?.cancelAndJoin() // stops its drain loop; handedOver keeps the link open
            val link = handover?.link?.also { it.push = push } ?: createLink(deviceId, push)
            synchronized(lock) {
                claiming -= deviceId
                claimed[deviceId] = link
            }
            return link
        } catch (t: Throwable) {
            synchronized(lock) { claiming -= deviceId }
            handover?.link?.close()
            throw t
        }
    }

    /**
     * Ends a session's claim. Unless the key was rejected, the link lingers and keeps uploading until its
     * buffer is empty, then closes. Must be called from a non-cancellable context, and only once nothing
     * of the session uses the link any more (its drain loop in particular: a link has one consumer).
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
            if (deviceId in claimed || deviceId in claiming || deviceId in lingering || !scope.isActive) return
            startLinger(createLink(deviceId) {})
        }
    }

    /** Must hold [lock]. */
    @OptIn(DelicateCoroutinesApi::class) // ATOMIC start: needed, and for the reason given below
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
                Slog.w("SpotflowGateway", "background upload for ${link.deviceId} stopped: ${t.message}")
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

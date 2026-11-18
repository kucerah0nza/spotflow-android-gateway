package io.spotflow.ble.transport

import android.annotation.SuppressLint
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothProfile
import android.bluetooth.BluetoothStatusCodes
import android.os.Build
import io.spotflow.ble.Slog
import io.spotflow.ble.protocol.FrameCodec
import io.spotflow.ble.protocol.GattProfile
import io.spotflow.ble.protocol.Message
import io.spotflow.ble.protocol.MessageType
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import java.io.IOException
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * The shared GATT engine used by both [ManagedBleConnection] and [AttachedBleConnection]. One session
 * serves one connection and is not reused.
 *
 * It owns the [BluetoothGattCallback] ([callback]) and serializes every GATT operation through a
 * [Mutex] so that only one is outstanding at a time — the single most important correctness rule of the
 * Android BLE stack.
 *
 * ## Attach-mode contract
 * In managed mode the library registers [callback] itself via `connectGatt`. In attach mode the host
 * app already owns the `BluetoothGatt` and its own callback; it must forward its callback events to
 * [callback] and run its own GATT operations inside [exclusive]. As a second line of defence, a callback
 * only completes the operation it answers: a read or write result for another characteristic — one of
 * the host's own — is ignored rather than taken for the gateway's.
 */
internal class SpotflowGattSession(
    private val requestedMtu: Int = MAX_MTU,
    /**
     * Counter a peripheral's too-short supervision timeout (see `onConnectionUpdated` in [callback]).
     * Managed mode only: an attached GATT's link parameters belong to the host.
     */
    private val guardSupervisionTimeout: Boolean = false,
) {
    companion object {
        private const val TAG = "SpotflowGatt"
        private const val LARGE_MESSAGE_BYTES = 4096
        private const val OP_TIMEOUT_MS = 10_000L
        private const val STOP_STREAMING_TIMEOUT_MS = 2_000L
        const val MAX_MTU = 517

        /**
         * Shortest supervision timeout tolerated from the peripheral, in 10 ms units (2 s). Shorter ones
         * drop the link on any brief radio gap (Wi-Fi coexistence, the phone in a pocket).
         */
        private const val MIN_SUPERVISION_TIMEOUT = 200
        private const val MAX_PRIORITY_REQUESTS = 3

        /** How long to wait for an ACK/NACK, and the backoff between retransmissions. */
        internal const val ACK_TIMEOUT_MS = 5_000L
        private const val ACK_RETRY_INITIAL_MS = 1_000L
        private const val ACK_RETRY_MAX_MS = 30_000L

        /** `GATT_AUTH_FAIL`: pairing failed or was declined (not in the public SDK). */
        private const val GATT_AUTH_FAIL = 0x89

        /**
         * A failed operation's GATT status, explained where the user has to act. A device can require an
         * encrypted link for everything but its Capabilities (the Device SDK does by default when it is
         * built with OTA), and then answers an unpaired gateway with one of these.
         */
        internal fun describeStatus(status: Int): String = when (status) {
            BluetoothGatt.GATT_INSUFFICIENT_AUTHENTICATION,
            BluetoothGatt.GATT_INSUFFICIENT_ENCRYPTION,
            GATT_AUTH_FAIL,
            -> "$status, the device requires an encrypted link: pair (bond) it with this phone"
            else -> status.toString()
        }
    }

    /** An outstanding operation on the characteristic [uuid], completed by the callback that answers it. */
    private class Pending<T>(val uuid: UUID) {
        val result = CompletableDeferred<T>()
    }

    /**
     * Awaits a GATT operation's callback, but fails (retryably) if it never arrives — a misbehaving
     * peripheral must not park a session forever. Not used for connect(), whose autoConnect waits by
     * design.
     */
    private suspend fun <T> CompletableDeferred<T>.awaitOp(operation: String): T =
        withTimeoutOrNull(OP_TIMEOUT_MS) { await() } ?: throw IOException("GATT $operation timed out")

    private val _state = MutableStateFlow(ConnectionState.DISCONNECTED)
    val state: StateFlow<ConnectionState> = _state

    // Unbounded so a burst of large coredump chunks is never dropped while MQTT publishing catches up.
    // The buffer is drained by the single relay pump and is naturally bounded by the coredump size.
    private val incomingChannel = Channel<Message>(Channel.UNLIMITED)
    val incoming: Flow<Message> = incomingChannel.receiveAsFlow()

    @Volatile
    var mtu: Int = FrameCodec.MIN_MTU
        private set

    // Only what a device sends: anything else on the TX Stream is dropped on arrival.
    private val reassembler = FrameCodec.Reassembler { it.isFromDevice }

    /** A message sent with NEEDS_ACK, waiting for the device's ACK (true) or NACK (false). */
    private class PendingAck(val seq: Int) {
        val result = CompletableDeferred<Boolean>()
    }

    /** Keyed by message type: the protocol allows one unacknowledged message per type. */
    private val pendingAcks = ConcurrentHashMap<Int, PendingAck>()

    // Serializes GATT operations. Only one operation is in flight, so a single pending slot suffices.
    private val opLock = Mutex()

    @Volatile private var pendingConnect: CompletableDeferred<Unit>? = null
    @Volatile private var pendingDiscover: CompletableDeferred<Unit>? = null
    @Volatile private var pendingMtu: CompletableDeferred<Int>? = null
    @Volatile private var pendingRead: Pending<ByteArray>? = null
    @Volatile private var pendingWrite: Pending<Unit>? = null

    /** Keyed by the characteristic whose CCCD is being written. */
    @Volatile private var pendingDescriptor: Pending<Unit>? = null
    @Volatile private var pendingRssi: CompletableDeferred<Int>? = null

    // Written by coroutines and read on binder threads (and the other way round).
    @Volatile private var gatt: BluetoothGatt? = null
    @Volatile private var closed = false
    @Volatile private var priorityRequests = 0

    /** Sequence number of the next message, per type: (type, seq) identifies a message. */
    private val txSeq = IntArray(256)

    private fun nextSeq(type: MessageType): Int = synchronized(txSeq) { txSeq[type.value]++ and 0xFF }

    fun attachGatt(gatt: BluetoothGatt) {
        this.gatt = gatt
    }

    /** The callback the library registers (managed) or the host forwards to (attached). */
    val callback: BluetoothGattCallback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
            if (!closed) this@SpotflowGattSession.gatt = gatt
            when (newState) {
                BluetoothProfile.STATE_CONNECTED -> {
                    priorityRequests = 0
                    if (status == BluetoothGatt.GATT_SUCCESS) {
                        pendingConnect?.complete(Unit)
                    } else {
                        fail("connection failed, status=$status")
                    }
                }
                BluetoothProfile.STATE_DISCONNECTED -> {
                    reassembler.reset()
                    _state.value = ConnectionState.DISCONNECTED
                    failAllPending(IllegalStateException("device disconnected, status=$status"))
                }
            }
        }

        override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
            if (status == BluetoothGatt.GATT_SUCCESS) {
                pendingDiscover?.complete(Unit)
            } else {
                pendingDiscover?.completeExceptionally(IllegalStateException("service discovery failed: $status"))
            }
        }

        override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
            this@SpotflowGattSession.mtu = if (status == BluetoothGatt.GATT_SUCCESS) mtu else FrameCodec.MIN_MTU
            pendingMtu?.complete(this@SpotflowGattSession.mtu)
        }

        // API 33+ delivers the value directly.
        override fun onCharacteristicRead(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            value: ByteArray,
            status: Int,
        ) = completeRead(characteristic.uuid, value, status)

        @Deprecated("Deprecated in API 33")
        @Suppress("DEPRECATION")
        override fun onCharacteristicRead(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            status: Int,
        ) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
                completeRead(characteristic.uuid, characteristic.value ?: ByteArray(0), status)
            }
        }

        override fun onCharacteristicWrite(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            status: Int,
        ) {
            val pending = pendingWrite?.takeIf { it.uuid == characteristic.uuid } ?: return
            if (status == BluetoothGatt.GATT_SUCCESS) {
                pending.result.complete(Unit)
            } else {
                pending.result.completeExceptionally(IllegalStateException("characteristic write failed: ${describeStatus(status)}"))
            }
        }

        override fun onDescriptorWrite(
            gatt: BluetoothGatt,
            descriptor: BluetoothGattDescriptor,
            status: Int,
        ) {
            if (descriptor.uuid != GattProfile.CCCD) return
            val pending = pendingDescriptor?.takeIf { it.uuid == descriptor.characteristic?.uuid } ?: return
            if (status == BluetoothGatt.GATT_SUCCESS) {
                pending.result.complete(Unit)
            } else {
                pending.result.completeExceptionally(IllegalStateException("descriptor write failed: ${describeStatus(status)}"))
            }
        }

        /**
         * Not in the public SDK (hidden since API 26), but the framework calls it on every link
         * parameter change, so a method with this exact signature receives it (no `override` keyword:
         * the SDK stubs don't declare it).
         *
         * Some peripherals request a very short supervision timeout (e.g. 420 ms). The link then drops
         * whenever the radio misses packets for that long, which happens constantly on a phone. Asking
         * Android for BALANCED priority renegotiates the link with Android's 5 s supervision timeout.
         * Capped per connection, so a peripheral that insists can't cause an endless renegotiation.
         */
        @Suppress("unused")
        fun onConnectionUpdated(gatt: BluetoothGatt, interval: Int, latency: Int, timeout: Int, status: Int) {
            if (!guardSupervisionTimeout || status != BluetoothGatt.GATT_SUCCESS) return
            if (timeout >= MIN_SUPERVISION_TIMEOUT || priorityRequests >= MAX_PRIORITY_REQUESTS) return
            priorityRequests++
            Slog.i(TAG, "peripheral set a ${timeout * 10} ms supervision timeout; requesting a longer one")
            @SuppressLint("MissingPermission")
            val requested = runCatching { gatt.requestConnectionPriority(BluetoothGatt.CONNECTION_PRIORITY_BALANCED) }
            if (requested.getOrNull() != true) Slog.w(TAG, "connection priority request rejected")
        }

        override fun onReadRemoteRssi(gatt: BluetoothGatt, rssi: Int, status: Int) {
            if (status == BluetoothGatt.GATT_SUCCESS) {
                pendingRssi?.complete(rssi)
            } else {
                pendingRssi?.completeExceptionally(IllegalStateException("readRemoteRssi failed: $status"))
            }
        }

        // API 33+ notification signature.
        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            value: ByteArray,
        ) = onNotification(characteristic, value)

        @Deprecated("Deprecated in API 33")
        @Suppress("DEPRECATION")
        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
        ) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
                onNotification(characteristic, characteristic.value ?: ByteArray(0))
            }
        }
    }

    private fun onNotification(characteristic: BluetoothGattCharacteristic, value: ByteArray) {
        if (characteristic.uuid != GattProfile.TX_STREAM) return
        val type = value.firstOrNull()?.toInt()?.and(0xFF)
        if (type == MessageType.ACK.value || type == MessageType.NACK.value) {
            // [type, acknowledged type, acknowledged seq]: completes the message waiting for it, if any.
            if (value.size >= 3) {
                val pending = pendingAcks[value[1].toInt() and 0xFF]
                if (pending != null && pending.seq == (value[2].toInt() and 0xFF)) {
                    pending.result.complete(type == MessageType.ACK.value)
                }
            }
            return
        }
        val message = reassembler.onFragment(value) ?: return
        if (message.payload.size >= LARGE_MESSAGE_BYTES) {
            // Large TELEMETRY messages are typically coredump chunks; log to aid diagnosis.
            Slog.d(TAG, "reassembled ${message.type} (${message.payload.size} bytes)")
        }
        incomingChannel.trySend(message)
    }

    /** Completes the pending read — if this result is for the characteristic it asked for. */
    private fun completeRead(uuid: UUID, value: ByteArray, status: Int) {
        val pending = pendingRead?.takeIf { it.uuid == uuid } ?: return
        if (status == BluetoothGatt.GATT_SUCCESS) {
            pending.result.complete(value.copyOf())
        } else {
            pending.result.completeExceptionally(
                IllegalStateException("${GattProfile.name(uuid)} read failed: ${describeStatus(status)}"),
            )
        }
    }

    private fun fail(reason: String) {
        Slog.w(TAG, reason)
        _state.value = ConnectionState.FAILED
        failAllPending(IllegalStateException(reason))
    }

    private fun failAllPending(cause: Throwable) {
        listOf(pendingConnect, pendingDiscover, pendingRead?.result, pendingWrite?.result, pendingDescriptor?.result, pendingRssi)
            .forEach { it?.completeExceptionally(cause) }
        pendingAcks.values.forEach { it.result.completeExceptionally(cause) }
        pendingMtu?.complete(FrameCodec.MIN_MTU)
    }

    // ---- serialized operations -------------------------------------------------------------------

    /**
     * Opens a GATT connection (managed mode). [open] receives [callback] and must return the
     * `BluetoothGatt` from `device.connectGatt(...)`. The pending slot is armed before [open] runs so
     * the connection callback can never be missed.
     */
    suspend fun connect(open: (BluetoothGattCallback) -> BluetoothGatt?) {
        val deferred = CompletableDeferred<Unit>()
        pendingConnect = deferred
        _state.value = ConnectionState.CONNECTING
        try {
            // connectGatt returns null e.g. when Bluetooth was turned off in the meantime.
            attachGatt(open(callback) ?: throw IllegalStateException("connectGatt() returned null"))
            deferred.await()
        } catch (t: Throwable) {
            // On failure or cancellation, release the GATT so any pending autoConnect attempt stops.
            disconnectAndClose()
            throw t
        } finally {
            pendingConnect = null
        }
    }

    /**
     * Attach mode: makes sure the host's GATT is connected before it is used — it may have been handed
     * over right after `connectGatt`, before the link is up. [isConnected] reports the link's state now;
     * if it is not up yet, the connection callback is awaited for at most [timeoutMs].
     */
    suspend fun awaitConnected(isConnected: () -> Boolean, timeoutMs: Long) {
        val deferred = CompletableDeferred<Unit>()
        pendingConnect = deferred // armed before the check, so the callback can't slip in between
        try {
            if (isConnected()) return
            _state.value = ConnectionState.CONNECTING
            withTimeoutOrNull(timeoutMs) { deferred.await() }
                ?: throw IOException("the attached GATT did not connect within ${timeoutMs / 1000} s")
        } finally {
            pendingConnect = null
        }
    }

    /**
     * Discover services and negotiate MTU. TX notifications are enabled separately ([startStreaming]),
     * after the gateway has read the device's characteristics — the order the Spotflow BLE protocol
     * specifies. Some devices don't answer reads once their TX stream is running.
     */
    @SuppressLint("MissingPermission")
    suspend fun prepare() = opLock.withLock {
        val g = requireGatt()
        _state.value = ConnectionState.PREPARING

        discover(g)
        negotiateMtu(g)
    }

    /** Enables TX Stream notifications; messages start flowing on [incoming]. */
    suspend fun startStreaming() = opLock.withLock {
        writeTxCccd(requireGatt(), enable = true)
        // Only from PREPARING: if the link dropped just now, the state says so and must keep saying so,
        // or whoever waits for the session to end would wait forever.
        check(_state.compareAndSet(ConnectionState.PREPARING, ConnectionState.READY)) {
            "device disconnected while its stream was being started"
        }
    }

    /**
     * Best effort: tells the device to stop its TX stream. For attach mode, where the link stays up after
     * the gateway lets go — the device would otherwise keep notifying, with nobody listening. Call from a
     * non-cancellable context when tearing down.
     */
    suspend fun stopStreaming() {
        if (_state.value != ConnectionState.READY) return
        val g = gatt ?: return
        val stopped = runCatching {
            withTimeoutOrNull(STOP_STREAMING_TIMEOUT_MS) { opLock.withLock { writeTxCccd(g, enable = false) } }
        }
        if (stopped.getOrNull() == null) Slog.w(TAG, "could not turn the TX stream off before detaching")
    }

    @SuppressLint("MissingPermission")
    private suspend fun discover(g: BluetoothGatt) {
        val deferred = CompletableDeferred<Unit>()
        pendingDiscover = deferred
        try {
            check(g.discoverServices()) { "discoverServices() rejected" }
            deferred.awaitOp("service discovery")
        } finally {
            pendingDiscover = null
        }
        checkNotNull(g.getService(GattProfile.SERVICE)) { "Spotflow service not found on device" }
    }

    @SuppressLint("MissingPermission")
    private suspend fun negotiateMtu(g: BluetoothGatt) {
        val deferred = CompletableDeferred<Int>()
        pendingMtu = deferred
        try {
            if (!g.requestMtu(requestedMtu)) {
                mtu = FrameCodec.MIN_MTU // fall back; not fatal
                return
            }
            // MTU is non-fatal: on timeout fall back to the minimum rather than failing prepare().
            if (withTimeoutOrNull(OP_TIMEOUT_MS) { deferred.await() } == null) {
                mtu = FrameCodec.MIN_MTU
            }
        } finally {
            pendingMtu = null
        }
    }

    /** Turns TX Stream notifications on or off, locally and on the device (its CCCD). */
    @SuppressLint("MissingPermission")
    private suspend fun writeTxCccd(g: BluetoothGatt, enable: Boolean) {
        val tx = characteristic(g, GattProfile.TX_STREAM)
        check(g.setCharacteristicNotification(tx, enable)) { "setCharacteristicNotification rejected" }
        val cccd = tx.getDescriptor(GattProfile.CCCD)
            ?: error("TX Stream is missing its CCCD descriptor")
        val value = if (enable) {
            BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
        } else {
            BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE
        }

        val pending = Pending<Unit>(GattProfile.TX_STREAM)
        pendingDescriptor = pending
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                val status = g.writeDescriptor(cccd, value)
                check(status == BluetoothStatusCodes.SUCCESS) { "writeDescriptor rejected: $status" }
            } else {
                @Suppress("DEPRECATION")
                run {
                    cccd.value = value
                    check(g.writeDescriptor(cccd)) { "writeDescriptor rejected" }
                }
            }
            pending.result.awaitOp("${if (enable) "enable" else "disable"} notifications (CCCD write)")
        } finally {
            pendingDescriptor = null
        }
    }

    @SuppressLint("MissingPermission")
    suspend fun read(uuid: UUID): ByteArray = opLock.withLock {
        val g = requireGatt()
        val ch = characteristic(g, uuid)
        val pending = Pending<ByteArray>(uuid)
        pendingRead = pending
        try {
            check(g.readCharacteristic(ch)) { "readCharacteristic($uuid) rejected" }
            pending.result.awaitOp("read ${GattProfile.name(uuid)}")
        } finally {
            pendingRead = null
        }
    }

    /** Reads the current connection signal strength (RSSI, dBm). */
    @SuppressLint("MissingPermission")
    suspend fun readRssi(): Int = opLock.withLock {
        val g = requireGatt()
        val deferred = CompletableDeferred<Int>()
        pendingRssi = deferred
        try {
            check(g.readRemoteRssi()) { "readRemoteRssi() rejected" }
            deferred.awaitOp("RSSI read")
        } finally {
            pendingRssi = null
        }
    }

    /** Fragments and writes one message of [type] to the RX Stream (best effort: no acknowledgement). */
    suspend fun writeMessage(type: MessageType, payload: ByteArray) {
        val frames = FrameCodec.fragment(type, payload, nextSeq(type), mtu)
        writeFrames(frames)
    }

    /**
     * Writes one message of [type] with NEEDS_ACK and suspends until the device acknowledges it. A NACK or
     * no answer within [ACK_TIMEOUT_MS] retransmits the complete message, with the same sequence number,
     * after a backoff — for as long as the link is up. Throws once it is not.
     */
    suspend fun writeAcknowledged(type: MessageType, payload: ByteArray) {
        val seq = nextSeq(type)
        val frames = FrameCodec.fragment(type, payload, seq, mtu, needsAck = true)
        var backoff = ACK_RETRY_INITIAL_MS
        while (true) {
            val pending = PendingAck(seq)
            // Armed before the write, so a quick answer can't slip past.
            pendingAcks[type.value] = pending
            try {
                writeFrames(frames)
                when (withTimeoutOrNull(ACK_TIMEOUT_MS) { pending.result.await() }) {
                    true -> return
                    false -> Slog.d(TAG, "device refused $type #$seq (NACK); retransmitting")
                    null -> Slog.d(TAG, "no acknowledgement of $type #$seq; retransmitting")
                }
            } finally {
                pendingAcks.remove(type.value, pending)
            }
            delay(backoff)
            backoff = (backoff * 2).coerceAtMost(ACK_RETRY_MAX_MS)
        }
    }

    @SuppressLint("MissingPermission")
    private suspend fun writeFrames(frames: List<ByteArray>) = opLock.withLock {
        val g = requireGatt()
        val rx = characteristic(g, GattProfile.RX_STREAM)
        for (frame in frames) {
            writeNoResponse(g, rx, frame)
        }
    }

    @SuppressLint("MissingPermission")
    private suspend fun writeNoResponse(g: BluetoothGatt, ch: BluetoothGattCharacteristic, value: ByteArray) {
        val pending = Pending<Unit>(ch.uuid)
        pendingWrite = pending
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                val status = g.writeCharacteristic(ch, value, BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE)
                check(status == BluetoothStatusCodes.SUCCESS) { "writeCharacteristic rejected: $status" }
            } else {
                @Suppress("DEPRECATION")
                run {
                    ch.writeType = BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE
                    ch.value = value
                    check(g.writeCharacteristic(ch)) { "writeCharacteristic rejected" }
                }
            }
            pending.result.awaitOp("write")
        } finally {
            pendingWrite = null
        }
    }

    /**
     * Runs [block] holding the GATT operation lock, so operations issued by the host inside it cannot
     * overlap with the gateway's own (attach mode).
     */
    suspend fun <T> exclusive(block: suspend () -> T): T = opLock.withLock { block() }

    @SuppressLint("MissingPermission")
    fun disconnectAndClose() {
        val g = gatt
        release()
        if (g != null) {
            runCatching { g.disconnect() }
            runCatching { g.close() }
        }
    }

    /** Stop using the (host-owned) GATT without disconnecting it. */
    fun detach() = release()

    /** Ends this session's use of the GATT; always leaves it DISCONNECTED with the stream closed. */
    private fun release() {
        closed = true
        gatt = null
        reassembler.reset()
        incomingChannel.close()
        _state.value = ConnectionState.DISCONNECTED
    }

    /** Whether this session has let go of its GATT for good. */
    val isClosed: Boolean get() = closed

    private fun requireGatt(): BluetoothGatt =
        gatt ?: throw IllegalStateException("no GATT attached")

    private fun characteristic(g: BluetoothGatt, uuid: UUID): BluetoothGattCharacteristic =
        g.getService(GattProfile.SERVICE)?.getCharacteristic(uuid)
            ?: error("characteristic $uuid not found in Spotflow service")
}

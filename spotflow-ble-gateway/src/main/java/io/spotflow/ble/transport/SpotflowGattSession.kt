package io.spotflow.ble.transport

import android.annotation.SuppressLint
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothProfile
import android.os.Build
import android.util.Log
import io.spotflow.ble.protocol.FrameCodec
import io.spotflow.ble.protocol.GattProfile
import io.spotflow.ble.protocol.Message
import io.spotflow.ble.protocol.MessageType
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * The shared GATT engine used by both [ManagedBleConnection] and [AttachedBleConnection].
 *
 * It owns the [BluetoothGattCallback] ([callback]) and serializes every GATT operation through a
 * [Mutex] so that only one is outstanding at a time — the single most important correctness rule of the
 * Android BLE stack.
 *
 * ## Attach-mode contract
 * In managed mode the library registers [callback] itself via `connectGatt`. In attach mode the host
 * app already owns the `BluetoothGatt` and its own callback; it MUST forward the relevant callback
 * events to [callback] (see [AttachedBleConnection]) and MUST NOT issue competing GATT operations while
 * a Spotflow session is active.
 */
internal class SpotflowGattSession(
    private val requestedMtu: Int = MAX_MTU,
) {
    companion object {
        private const val TAG = "SpotflowGatt"
        const val MAX_MTU = 517
        private val CCCD_ENABLE_NOTIFICATION = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
    }

    private val _state = MutableStateFlow(ConnectionState.DISCONNECTED)
    val state: StateFlow<ConnectionState> = _state

    private val _incoming = MutableSharedFlow<Message>(extraBufferCapacity = 64)
    val incoming: SharedFlow<Message> = _incoming

    @Volatile
    var mtu: Int = FrameCodec.MIN_MTU
        private set

    private val reassembler = FrameCodec.Reassembler()

    // Serializes GATT operations. Only one operation is in flight, so a single pending slot suffices.
    private val opLock = Mutex()

    @Volatile private var pendingConnect: CompletableDeferred<Unit>? = null
    @Volatile private var pendingDiscover: CompletableDeferred<Unit>? = null
    @Volatile private var pendingMtu: CompletableDeferred<Int>? = null
    @Volatile private var pendingRead: CompletableDeferred<ByteArray>? = null
    @Volatile private var pendingWrite: CompletableDeferred<Unit>? = null
    @Volatile private var pendingDescriptor: CompletableDeferred<Unit>? = null

    private var gatt: BluetoothGatt? = null
    private var txSeq = 0

    fun attachGatt(gatt: BluetoothGatt) {
        this.gatt = gatt
    }

    /** The callback the library registers (managed) or the host forwards to (attached). */
    val callback: BluetoothGattCallback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
            this@SpotflowGattSession.gatt = gatt
            when (newState) {
                BluetoothProfile.STATE_CONNECTED -> {
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
        ) = completeRead(value, status)

        @Deprecated("Deprecated in API 33")
        @Suppress("DEPRECATION")
        override fun onCharacteristicRead(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            status: Int,
        ) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
                completeRead(characteristic.value ?: ByteArray(0), status)
            }
        }

        override fun onCharacteristicWrite(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            status: Int,
        ) {
            if (status == BluetoothGatt.GATT_SUCCESS) {
                pendingWrite?.complete(Unit)
            } else {
                pendingWrite?.completeExceptionally(IllegalStateException("characteristic write failed: $status"))
            }
        }

        override fun onDescriptorWrite(
            gatt: BluetoothGatt,
            descriptor: BluetoothGattDescriptor,
            status: Int,
        ) {
            if (status == BluetoothGatt.GATT_SUCCESS) {
                pendingDescriptor?.complete(Unit)
            } else {
                pendingDescriptor?.completeExceptionally(IllegalStateException("descriptor write failed: $status"))
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
        val message = reassembler.onFragment(value) ?: return
        if (!_incoming.tryEmit(message)) {
            Log.w(TAG, "dropped inbound ${message.type}: subscriber buffer full")
        }
    }

    private fun completeRead(value: ByteArray, status: Int) {
        if (status == BluetoothGatt.GATT_SUCCESS) {
            pendingRead?.complete(value.copyOf())
        } else {
            pendingRead?.completeExceptionally(IllegalStateException("characteristic read failed: $status"))
        }
    }

    private fun fail(reason: String) {
        Log.w(TAG, reason)
        _state.value = ConnectionState.FAILED
        failAllPending(IllegalStateException(reason))
    }

    private fun failAllPending(cause: Throwable) {
        listOf(pendingConnect, pendingDiscover, pendingRead, pendingWrite, pendingDescriptor)
            .forEach { it?.completeExceptionally(cause) }
        pendingMtu?.complete(FrameCodec.MIN_MTU)
    }

    // ---- serialized operations -------------------------------------------------------------------

    /**
     * Opens a GATT connection (managed mode). [open] receives [callback] and must return the
     * `BluetoothGatt` from `device.connectGatt(...)`. The pending slot is armed before [open] runs so
     * the connection callback can never be missed.
     */
    suspend fun connect(open: (BluetoothGattCallback) -> BluetoothGatt) {
        val deferred = CompletableDeferred<Unit>()
        pendingConnect = deferred
        _state.value = ConnectionState.CONNECTING
        attachGatt(open(callback))
        try {
            deferred.await()
        } catch (t: Throwable) {
            // On failure or cancellation, release the GATT so any pending autoConnect attempt stops.
            disconnectAndClose()
            throw t
        } finally {
            pendingConnect = null
        }
    }

    /** Discover services, negotiate MTU, and enable TX notifications. */
    @SuppressLint("MissingPermission")
    suspend fun prepare() = opLock.withLock {
        val g = requireGatt()
        _state.value = ConnectionState.PREPARING

        discover(g)
        negotiateMtu(g)
        enableTxNotifications(g)

        _state.value = ConnectionState.READY
    }

    @SuppressLint("MissingPermission")
    private suspend fun discover(g: BluetoothGatt) {
        val deferred = CompletableDeferred<Unit>()
        pendingDiscover = deferred
        try {
            check(g.discoverServices()) { "discoverServices() rejected" }
            deferred.await()
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
            deferred.await()
        } finally {
            pendingMtu = null
        }
    }

    @SuppressLint("MissingPermission")
    private suspend fun enableTxNotifications(g: BluetoothGatt) {
        val tx = characteristic(g, GattProfile.TX_STREAM)
        check(g.setCharacteristicNotification(tx, true)) { "setCharacteristicNotification rejected" }
        val cccd = tx.getDescriptor(GattProfile.CCCD)
            ?: error("TX Stream is missing its CCCD descriptor")

        val deferred = CompletableDeferred<Unit>()
        pendingDescriptor = deferred
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                g.writeDescriptor(cccd, CCCD_ENABLE_NOTIFICATION)
            } else {
                @Suppress("DEPRECATION")
                run {
                    cccd.value = CCCD_ENABLE_NOTIFICATION
                    g.writeDescriptor(cccd)
                }
            }
            deferred.await()
        } finally {
            pendingDescriptor = null
        }
    }

    @SuppressLint("MissingPermission")
    suspend fun read(uuid: java.util.UUID): ByteArray = opLock.withLock {
        val g = requireGatt()
        val ch = characteristic(g, uuid)
        val deferred = CompletableDeferred<ByteArray>()
        pendingRead = deferred
        try {
            check(g.readCharacteristic(ch)) { "readCharacteristic($uuid) rejected" }
            deferred.await()
        } finally {
            pendingRead = null
        }
    }

    @SuppressLint("MissingPermission")
    suspend fun writeDesiredConfiguration(payload: ByteArray) = opLock.withLock {
        val g = requireGatt()
        val rx = characteristic(g, GattProfile.RX_STREAM)
        val seq = txSeq++ and 0xFF
        val frames = FrameCodec.fragment(MessageType.DESIRED_CONFIGURATION, payload, seq, mtu)
        for (frame in frames) {
            writeNoResponse(g, rx, frame)
        }
    }

    @SuppressLint("MissingPermission")
    private suspend fun writeNoResponse(g: BluetoothGatt, ch: BluetoothGattCharacteristic, value: ByteArray) {
        val deferred = CompletableDeferred<Unit>()
        pendingWrite = deferred
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                val status = g.writeCharacteristic(ch, value, BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE)
                check(status == BluetoothGatt.GATT_SUCCESS) { "writeCharacteristic rejected: $status" }
            } else {
                @Suppress("DEPRECATION")
                run {
                    ch.writeType = BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE
                    ch.value = value
                    check(g.writeCharacteristic(ch)) { "writeCharacteristic rejected" }
                }
            }
            deferred.await()
        } finally {
            pendingWrite = null
        }
    }

    @SuppressLint("MissingPermission")
    fun disconnectAndClose() {
        val g = gatt ?: return
        runCatching { g.disconnect() }
        runCatching { g.close() }
        gatt = null
        reassembler.reset()
        _state.value = ConnectionState.DISCONNECTED
    }

    /** Stop using the (host-owned) GATT without disconnecting it. */
    fun detach() {
        gatt = null
        reassembler.reset()
        _state.value = ConnectionState.DISCONNECTED
    }

    private fun requireGatt(): BluetoothGatt =
        gatt ?: throw IllegalStateException("no GATT attached")

    private fun characteristic(g: BluetoothGatt, uuid: java.util.UUID): BluetoothGattCharacteristic =
        g.getService(GattProfile.SERVICE)?.getCharacteristic(uuid)
            ?: error("characteristic $uuid not found in Spotflow service")
}

package io.spotflow.ble.transport

import android.annotation.SuppressLint
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothManager
import android.content.Context
import io.spotflow.ble.protocol.GattProfile
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.withTimeoutOrNull

/**
 * A [BleConnection] the library fully owns: it opens the GATT connection to [device], negotiates the
 * MTU, and manages disconnection. Use this when the host app has no existing BLE connection of its own.
 *
 * Discovery of the [device] (scanning for [GattProfile.SERVICE]) is handled by [SpotflowScanner].
 */
class ManagedBleConnection(
    context: Context,
    private val device: BluetoothDevice,
    requestedMtu: Int = SpotflowGattSession.MAX_MTU,
    /**
     * `false` for a direct connect to an in-range device (fast, ideal for the first attempt after a
     * scan). `true` lets Android re-attach automatically whenever the device reappears — the right
     * choice for reconnecting to a known device that may currently be down.
     */
    private val autoConnect: Boolean = false,
    /**
     * With [autoConnect], how long to wait for the device to reappear before giving up with
     * [DeviceUnreachableException] — so a device that is gone for good doesn't hold one of Android's
     * limited GATT client slots forever. `0` waits indefinitely.
     */
    private val autoConnectTimeoutMs: Long = DEFAULT_AUTO_CONNECT_TIMEOUT_MS,
) : BleConnection {

    private val context = context.applicationContext

    private val session = SpotflowGattSession(requestedMtu)

    override val deviceAddress: String get() = device.address
    override val state: StateFlow<ConnectionState> get() = session.state
    override val incoming: Flow<io.spotflow.ble.protocol.Message> get() = session.incoming
    override val mtu: Int get() = session.mtu

    // The host is responsible for holding BLUETOOTH_CONNECT (documented in the README/manifest).
    @SuppressLint("MissingPermission")
    override suspend fun prepare() {
        // connectGatt while Bluetooth is off returns null / never connects; fail fast so the caller's
        // retry loop backs off and reconnects once Bluetooth is back on.
        val adapter = (context.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter
        check(adapter?.isEnabled == true) { "Bluetooth is off" }
        val connect = suspend {
            session.connect { callback ->
                device.connectGatt(context, autoConnect, callback, BluetoothDevice.TRANSPORT_LE)
            }
        }
        if (autoConnect && autoConnectTimeoutMs > 0) {
            withTimeoutOrNull(autoConnectTimeoutMs) { connect() }
                ?: throw DeviceUnreachableException("not seen for ${autoConnectTimeoutMs / 1000}s")
        } else {
            connect()
        }
        session.prepare()
    }

    override suspend fun readDeviceId(): String =
        session.read(GattProfile.DEVICE_ID).toString(Charsets.UTF_8).trim()

    override suspend fun readSessionMetadata(): ByteArray =
        session.read(GattProfile.SESSION_METADATA)

    override suspend fun readRssi(): Int = session.readRssi()

    override suspend fun sendDesiredConfiguration(payload: ByteArray) =
        session.writeDesiredConfiguration(payload)

    override suspend fun close() = session.disconnectAndClose()

    companion object {
        const val DEFAULT_AUTO_CONNECT_TIMEOUT_MS = 10 * 60 * 1000L
    }
}

/** A managed device did not reappear within its auto-connect timeout. */
class DeviceUnreachableException(message: String) : Exception(message)

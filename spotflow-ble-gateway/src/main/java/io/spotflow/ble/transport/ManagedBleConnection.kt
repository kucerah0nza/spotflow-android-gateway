package io.spotflow.ble.transport

import android.annotation.SuppressLint
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothManager
import android.content.Context
import io.spotflow.ble.protocol.GattProfile
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

/**
 * A [BleConnection] the library fully owns: it opens the GATT connection to [device], negotiates the
 * MTU, and manages disconnection. Use this when the host app has no existing BLE connection of its own.
 *
 * Discovery of the [device] (scanning for [GattProfile.SERVICE]) is handled by [SpotflowScanner].
 */
class ManagedBleConnection(
    private val context: Context,
    private val device: BluetoothDevice,
    requestedMtu: Int = SpotflowGattSession.MAX_MTU,
    /**
     * `false` for a direct connect to an in-range device (fast, ideal for the first attempt after a
     * scan). `true` lets Android re-attach automatically whenever the device reappears — the right
     * choice for reconnecting to a known device that may currently be down.
     */
    private val autoConnect: Boolean = false,
) : BleConnection {

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
        session.connect { callback ->
            device.connectGatt(context, autoConnect, callback, BluetoothDevice.TRANSPORT_LE)
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
}

package io.spotflow.ble.transport

import android.bluetooth.BluetoothDevice
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
) : BleConnection {

    private val session = SpotflowGattSession(requestedMtu)

    override val deviceAddress: String get() = device.address
    override val state: StateFlow<ConnectionState> get() = session.state
    override val incoming: Flow<io.spotflow.ble.protocol.Message> get() = session.incoming
    override val mtu: Int get() = session.mtu

    override suspend fun prepare() {
        session.connect { callback ->
            device.connectGatt(context, /* autoConnect = */ false, callback, BluetoothDevice.TRANSPORT_LE)
        }
        session.prepare()
    }

    override suspend fun readDeviceId(): String =
        session.read(GattProfile.DEVICE_ID).toString(Charsets.UTF_8).trim()

    override suspend fun readSessionMetadata(): ByteArray =
        session.read(GattProfile.SESSION_METADATA)

    override suspend fun sendDesiredConfiguration(payload: ByteArray) =
        session.writeDesiredConfiguration(payload)

    override suspend fun close() = session.disconnectAndClose()
}

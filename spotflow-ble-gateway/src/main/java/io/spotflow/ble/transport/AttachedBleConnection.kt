package io.spotflow.ble.transport

import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import io.spotflow.ble.protocol.GattProfile
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

/**
 * A [BleConnection] that attaches to a `BluetoothGatt` the host app already owns — e.g. a smart
 * thermostat app that is already connected to its device and does not want a second GATT connection.
 *
 * ## Host contract
 * The host retains ownership of connect/disconnect and MUST:
 *  1. Forward its `BluetoothGattCallback` events to [gattCallback] (use it directly as the connectGatt
 *     callback, or fan out to it from the host's own callback), and
 *  2. Not issue competing GATT operations while a Spotflow session is active (the Android stack allows
 *     only one outstanding GATT operation at a time).
 *
 * [prepare] assumes the GATT is already connected; it discovers services, negotiates MTU, and enables
 * TX notifications. [close] detaches without disconnecting the host's GATT.
 */
class AttachedBleConnection(
    private val gatt: BluetoothGatt,
    requestedMtu: Int = SpotflowGattSession.MAX_MTU,
) : BleConnection {

    private val session = SpotflowGattSession(requestedMtu).apply { attachGatt(gatt) }

    /** The callback the host app must forward GATT events to. */
    val gattCallback: BluetoothGattCallback get() = session.callback

    override val deviceAddress: String get() = gatt.device.address
    override val state: StateFlow<ConnectionState> get() = session.state
    override val incoming: Flow<io.spotflow.ble.protocol.Message> get() = session.incoming
    override val mtu: Int get() = session.mtu

    override suspend fun prepare() = session.prepare()

    override suspend fun readDeviceId(): String =
        session.read(GattProfile.DEVICE_ID).toString(Charsets.UTF_8).trim()

    override suspend fun readSessionMetadata(): ByteArray =
        session.read(GattProfile.SESSION_METADATA)

    override suspend fun readRssi(): Int = session.readRssi()

    override suspend fun sendDesiredConfiguration(payload: ByteArray) =
        session.writeDesiredConfiguration(payload)

    override suspend fun close() = session.detach()
}

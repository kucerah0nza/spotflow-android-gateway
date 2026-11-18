package io.spotflow.ble.transport

import android.bluetooth.BluetoothGatt
import io.spotflow.ble.protocol.DeviceCapabilities
import io.spotflow.ble.protocol.GattProfile
import io.spotflow.ble.protocol.Message
import io.spotflow.ble.protocol.MessageType
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

/**
 * A [BleConnection] on a `BluetoothGatt` the host app owns — e.g. a smart thermostat app that is already
 * connected to its device and does not want a second GATT connection. Hosts reach it through
 * `GattAttachment`, which documents the contract (forwarding callbacks, `runExclusive`).
 *
 * [prepare] waits for the GATT to be connected if it isn't yet, then discovers services and negotiates
 * MTU. [close] turns the device's TX stream off and detaches, without disconnecting the host's GATT.
 */
internal class AttachedBleConnection(
    private val gatt: BluetoothGatt,
    private val session: SpotflowGattSession,
    /** Whether the host's GATT link is up right now. */
    private val isLinkUp: () -> Boolean,
) : BleConnection {

    override val deviceAddress: String get() = gatt.device.address
    override val state: StateFlow<ConnectionState> get() = session.state
    override val incoming: Flow<Message> get() = session.incoming
    override val mtu: Int get() = session.mtu

    override suspend fun prepare() {
        session.attachGatt(gatt)
        session.awaitConnected(isLinkUp, CONNECT_WAIT_MS)
        session.prepare()
    }

    override suspend fun readCapabilities(): DeviceCapabilities =
        DeviceCapabilities.parse(session.read(GattProfile.CAPABILITIES))

    override suspend fun startStreaming() = session.startStreaming()

    override suspend fun readDeviceId(): String =
        session.read(GattProfile.DEVICE_ID).toString(Charsets.UTF_8).trim()

    override suspend fun readSessionMetadata(): ByteArray =
        session.read(GattProfile.SESSION_METADATA)

    override suspend fun readRssi(): Int = session.readRssi()

    override suspend fun sendDesiredConfiguration(payload: ByteArray) =
        session.writeMessage(MessageType.DESIRED_CONFIGURATION, payload)

    override suspend fun sendOtaCommand(payload: ByteArray) =
        session.writeAcknowledged(MessageType.OTA_C2D, payload)

    override suspend fun sendArtifactResponse(payload: ByteArray) =
        session.writeMessage(MessageType.ARTIFACT_RESPONSE, payload)

    override suspend fun close() {
        // The host's link stays up after us: without this the device would keep streaming to nobody.
        session.stopStreaming()
        session.detach()
    }

    /** Runs [block] while holding the GATT operation queue; see `GattAttachment.runExclusive`. */
    suspend fun <T> runExclusive(block: suspend (BluetoothGatt) -> T): T = session.exclusive { block(gatt) }

    private companion object {
        /** How long a GATT handed over before it is connected may take to connect. */
        const val CONNECT_WAIT_MS = 30_000L
    }
}

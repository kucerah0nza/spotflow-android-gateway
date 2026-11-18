package io.spotflow.ble.transport

import io.spotflow.ble.protocol.DeviceCapabilities
import io.spotflow.ble.protocol.Message
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

/** Lifecycle state of a single BLE connection to a Spotflow device. */
public enum class ConnectionState {
    /** Not connected (initial state, or after the session ended). */
    DISCONNECTED,

    /** GATT link is being established. */
    CONNECTING,

    /** Discovering services / negotiating MTU / reading characteristics. */
    PREPARING,

    /** Ready: notifications enabled, characteristics resolved, messages flowing. */
    READY,

    /** Terminal failure for this attempt; a managed connection is retried. */
    FAILED,
}

/**
 * A transport to one Spotflow device that exposes the framed message streams, independent of whether
 * the library owns the BLE connection ([ManagedBleConnection]) or attaches to one the host app already
 * holds ([AttachedBleConnection]).
 *
 * All suspend functions serialize onto the single GATT operation queue; only one GATT operation is ever
 * outstanding at a time.
 */
internal interface BleConnection {

    /** Bluetooth MAC address of the device. */
    val deviceAddress: String

    /** Observable connection state. */
    val state: StateFlow<ConnectionState>

    /** Reassembled messages arriving on the TX Stream (device -> gateway). */
    val incoming: Flow<Message>

    /** The negotiated ATT MTU, or 23 before negotiation completes. */
    val mtu: Int

    /**
     * Establishes the link: for managed connections this connects the GATT; for attached connections it
     * uses the host-provided GATT. In both cases it discovers services and negotiates MTU. Messages only
     * start flowing after [startStreaming].
     */
    suspend fun prepare()

    /** Reads the Capabilities characteristic: protocol version and feature bits. */
    suspend fun readCapabilities(): DeviceCapabilities

    /** Enables TX Stream notifications; [state] then reaches [ConnectionState.READY]. */
    suspend fun startStreaming()

    /** Reads the device ID (MQTT username) from the Device ID characteristic. */
    suspend fun readDeviceId(): String

    /** Reads the CBOR session-metadata characteristic. */
    suspend fun readSessionMetadata(): ByteArray

    /** Reads the current connection signal strength (RSSI, in dBm). */
    suspend fun readRssi(): Int

    /** Fragments and writes a DESIRED_CONFIGURATION payload to the RX Stream. */
    suspend fun sendDesiredConfiguration(payload: ByteArray)

    /**
     * Writes an OTA command (OTA_C2D) and suspends until the device acknowledges it, retransmitting on a
     * NACK or silence. Throws if the link goes down first.
     */
    suspend fun sendOtaCommand(payload: ByteArray)

    /** Fragments and writes an ARTIFACT_RESPONSE payload to the RX Stream. */
    suspend fun sendArtifactResponse(payload: ByteArray)

    /**
     * Releases the transport. A managed connection disconnects and closes the GATT; an attached
     * connection turns the device's TX stream off, stops using the GATT and leaves the host to own
     * disconnection. Call from a non-cancellable context.
     */
    suspend fun close()
}

package io.spotflow.ble.transport

import io.spotflow.ble.protocol.Message
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

/** Lifecycle state of a single BLE connection to a Spotflow device. */
enum class ConnectionState {
    /** Not connected (initial state, or after [BleConnection.close]). */
    DISCONNECTED,

    /** GATT link is being established (managed mode only). */
    CONNECTING,

    /** Discovering services / negotiating MTU / enabling notifications. */
    PREPARING,

    /** Ready: notifications enabled, characteristics resolved, messages flowing. */
    READY,

    /** Terminal failure for this attempt; a managed connection may retry. */
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
interface BleConnection {

    /** Bluetooth MAC address of the device. */
    val deviceAddress: String

    /** Observable connection state. */
    val state: StateFlow<ConnectionState>

    /** Reassembled messages arriving on the TX Stream (device -> gateway). */
    val incoming: Flow<Message>

    /** The negotiated ATT MTU, or 23 before negotiation completes. */
    val mtu: Int

    /**
     * Establishes readiness: for managed connections this connects the GATT; for attached connections
     * it discovers services on the host-provided GATT. In both cases it negotiates MTU and enables TX
     * notifications. Returns once [state] reaches [ConnectionState.READY].
     */
    suspend fun prepare()

    /** Reads the device ID (MQTT username) from the Device ID characteristic. */
    suspend fun readDeviceId(): String

    /** Reads the CBOR session-metadata characteristic. */
    suspend fun readSessionMetadata(): ByteArray

    /** Reads the current connection signal strength (RSSI, in dBm). */
    suspend fun readRssi(): Int

    /** Fragments and writes a DESIRED_CONFIGURATION payload to the RX Stream. */
    suspend fun sendDesiredConfiguration(payload: ByteArray)

    /**
     * Releases the transport. A managed connection disconnects and closes the GATT; an attached
     * connection only stops using it and leaves the host to own disconnection.
     */
    suspend fun close()
}

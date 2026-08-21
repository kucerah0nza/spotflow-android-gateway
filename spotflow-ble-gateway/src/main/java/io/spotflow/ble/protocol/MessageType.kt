package io.spotflow.ble.protocol

/**
 * Message types carried by the Spotflow BLE framed protocol, identified by the first byte of a frame.
 *
 * Direction is relative to the gateway:
 *  - [TELEMETRY] and [REPORTED_CONFIGURATION] arrive on the TX Stream (device -> gateway).
 *  - [DESIRED_CONFIGURATION] is sent on the RX Stream (gateway -> device).
 *  - [ACK]/[NACK] are defined by the protocol but acknowledgement is not currently implemented
 *    (delivery is best-effort).
 */
enum class MessageType(val value: Int) {
    ACK(0x00),
    NACK(0x01),
    TELEMETRY(0x02),
    REPORTED_CONFIGURATION(0x03),
    DESIRED_CONFIGURATION(0x04);

    companion object {
        fun fromValue(value: Int): MessageType? = entries.firstOrNull { it.value == value }
    }
}

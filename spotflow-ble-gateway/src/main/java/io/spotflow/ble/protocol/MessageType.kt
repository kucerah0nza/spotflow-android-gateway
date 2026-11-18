package io.spotflow.ble.protocol

/**
 * Message types carried by the Spotflow BLE framed protocol (version 1.1), identified by the first byte of
 * a frame.
 *
 * Direction is relative to the gateway:
 *  - [TELEMETRY] and [REPORTED_CONFIGURATION] arrive on the TX Stream (device -> gateway) and are relayed.
 *  - [DESIRED_CONFIGURATION] is sent on the RX Stream (gateway -> device).
 *  - [ACK]/[NACK] are unfragmented 3-byte control frames (`type, acknowledged type, acknowledged seq`)
 *    answering a message sent with [FrameCodec.FLAG_NEEDS_ACK]. The gateway sends no such message, so it
 *    receives none; any that arrive are ignored.
 *  - [OTA_D2C], [OTA_C2D], [ARTIFACT_REQUEST] and [ARTIFACT_RESPONSE] carry over-the-air updates, which
 *    this gateway does not relay yet: what a device sends of them is dropped (see [isRelayed]).
 */
internal enum class MessageType(val value: Int) {
    ACK(0x00),
    NACK(0x01),
    TELEMETRY(0x02),
    REPORTED_CONFIGURATION(0x03),
    DESIRED_CONFIGURATION(0x04),
    OTA_D2C(0x05),
    OTA_C2D(0x06),
    ARTIFACT_REQUEST(0x07),
    ARTIFACT_RESPONSE(0x08);

    /** Whether this is an unfragmented ACK/NACK control frame rather than a framed message. */
    val isControl: Boolean get() = this == ACK || this == NACK

    /** Whether the gateway relays this type when a device sends it on the TX Stream. */
    val isRelayed: Boolean get() = this == TELEMETRY || this == REPORTED_CONFIGURATION

    companion object {
        fun fromValue(value: Int): MessageType? = entries.firstOrNull { it.value == value }
    }
}

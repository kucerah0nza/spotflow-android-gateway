package io.spotflow.ble.protocol

/**
 * Message types carried by the Spotflow BLE framed protocol (version 1.1), identified by the first byte of
 * a frame.
 *
 * Direction is relative to the gateway:
 *  - [TELEMETRY] and [REPORTED_CONFIGURATION] arrive on the TX Stream (device -> gateway) and are relayed.
 *  - [DESIRED_CONFIGURATION] is sent on the RX Stream (gateway -> device).
 *  - [ACK]/[NACK] are unfragmented 3-byte control frames (`type, acknowledged type, acknowledged seq`)
 *    with which the device answers a message sent with [FrameCodec.FLAG_NEEDS_ACK] — an [OTA_C2D].
 *  - [OTA_C2D] carries OTA commands to the device, [OTA_D2C] its update results back.
 *  - [ARTIFACT_REQUEST] (device) and [ARTIFACT_RESPONSE] (gateway) carry an OTA artifact's transfer; see
 *    [ArtifactProtocol].
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

    /** Whether a device sends this type, on the TX Stream (control frames aside). */
    val isFromDevice: Boolean
        get() = this == TELEMETRY || this == REPORTED_CONFIGURATION || this == OTA_D2C || this == ARTIFACT_REQUEST

    companion object {
        fun fromValue(value: Int): MessageType? = entries.firstOrNull { it.value == value }
    }
}

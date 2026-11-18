package io.spotflow.ble.protocol

import java.util.UUID

/**
 * UUIDs of the Spotflow Observability GATT service and its characteristics.
 *
 * See https://docs.spotflow.io/fundamentals/bluetooth-low-energy
 */
public object GattProfile {

    /** The Spotflow Observability Service advertised by a device running the Device SDK. */
    public val SERVICE: UUID = UUID.fromString("26530001-81E5-4861-82AE-2C92E6887922")

    /**
     * Read-only. Protocol version (major, minor) and, from protocol 1.1, a feature byte (bit 0: the device
     * takes OTA updates through the gateway). Never requires an encrypted link.
     */
    public val CAPABILITIES: UUID = UUID.fromString("26530002-81E5-4861-82AE-2C92E6887922")

    /** Read-only. The device ID; used verbatim as the MQTT username towards the Spotflow cloud. */
    public val DEVICE_ID: UUID = UUID.fromString("26530003-81E5-4861-82AE-2C92E6887922")

    /** Read-only. CBOR-encoded session metadata, published to `ingest-cbor` when a session opens. */
    public val SESSION_METADATA: UUID = UUID.fromString("26530004-81E5-4861-82AE-2C92E6887922")

    /** Notify. Device -> gateway stream (telemetry, reported configuration; OTA messages in 1.1). */
    public val TX_STREAM: UUID = UUID.fromString("26530005-81E5-4861-82AE-2C92E6887922")

    /** Write without response. Gateway -> device stream (desired configuration; OTA messages in 1.1). */
    public val RX_STREAM: UUID = UUID.fromString("26530006-81E5-4861-82AE-2C92E6887922")

    /** Client Characteristic Configuration Descriptor, used to enable notifications on TX_STREAM. */
    public val CCCD: UUID = UUID.fromString("00002902-0000-1000-8000-00805F9B34FB")

    /** A readable name for [uuid], for logs and error messages. */
    public fun name(uuid: UUID): String = when (uuid) {
        SERVICE -> "Spotflow service"
        CAPABILITIES -> "Capabilities"
        DEVICE_ID -> "Device ID"
        SESSION_METADATA -> "Session Metadata"
        TX_STREAM -> "TX Stream"
        RX_STREAM -> "RX Stream"
        CCCD -> "CCCD"
        else -> uuid.toString()
    }
}

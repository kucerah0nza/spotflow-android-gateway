package io.spotflow.ble.protocol

import java.util.UUID

/**
 * UUIDs of the Spotflow Observability GATT service and its characteristics.
 *
 * See https://docs.spotflow.io/fundamentals/bluetooth-low-energy
 */
object GattProfile {

    /** The Spotflow Observability Service advertised by a device running the Device SDK. */
    val SERVICE: UUID = UUID.fromString("26530001-81E5-4861-82AE-2C92E6887922")

    /** Read-only. Device capabilities (which message types / features are supported). */
    val CAPABILITIES: UUID = UUID.fromString("26530002-81E5-4861-82AE-2C92E6887922")

    /** Read-only. The device ID; used verbatim as the MQTT username towards the Spotflow cloud. */
    val DEVICE_ID: UUID = UUID.fromString("26530003-81E5-4861-82AE-2C92E6887922")

    /** Read-only. CBOR-encoded session metadata, published to `ingest-cbor` when a session opens. */
    val SESSION_METADATA: UUID = UUID.fromString("26530004-81E5-4861-82AE-2C92E6887922")

    /** Notify. Device -> gateway stream (telemetry, reported configuration). */
    val TX_STREAM: UUID = UUID.fromString("26530005-81E5-4861-82AE-2C92E6887922")

    /** Write without response. Gateway -> device stream (desired configuration). */
    val RX_STREAM: UUID = UUID.fromString("26530006-81E5-4861-82AE-2C92E6887922")

    /** Client Characteristic Configuration Descriptor, used to enable notifications on TX_STREAM. */
    val CCCD: UUID = UUID.fromString("00002902-0000-1000-8000-00805F9B34FB")
}

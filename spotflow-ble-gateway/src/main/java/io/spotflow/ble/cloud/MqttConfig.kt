package io.spotflow.ble.cloud

/**
 * MQTT topic names on the Spotflow broker. Telemetry, reported configuration and OTA results are published
 * to fixed topics; desired configuration and OTA commands arrive on a per-device topic *below* the one
 * subscribed to (`config-cbor-c2d/<workspace>/<device>`), so those are matched by prefix.
 */
public data class SpotflowTopics(
    val ingest: String = "ingest-cbor",
    val reportedConfiguration: String = "config-cbor-d2c",
    val desiredConfiguration: String = "config-cbor-c2d",
    /** OTA commands for the device (subscribed to only for devices that take OTA updates). */
    val otaCommands: String = "ota-cbor-c2d",
    /** The device's OTA update results. */
    val otaResults: String = "ota-cbor-d2c",
)

/** MQTT delivery guarantee for the messages the gateway publishes. */
public enum class PublishQos {
    /** Fire and forget: a message is dropped from the buffer as soon as it is written to the socket. */
    AT_MOST_ONCE,

    /** A message stays buffered until the broker acknowledges it; it may be delivered twice. */
    AT_LEAST_ONCE,

    /** Like [AT_LEAST_ONCE], with the broker de-duplicating within one connection. */
    EXACTLY_ONCE,
}

/**
 * Connection settings for the Spotflow MQTT broker. Defaults target production: `mqtt.spotflow.io:8883`
 * over TLS (Let's Encrypt ISRG Root X1, validated by the Android system trust store) with QoS 1.
 */
public data class MqttConfig(
    val host: String = "mqtt.spotflow.io",
    val port: Int = 8883,
    val useTls: Boolean = true,
    /**
     * MQTT keep-alive. Every device has its own connection, each pinging at this interval while idle, so
     * a short one costs battery on mobile data. A dead link is noticed sooner than this whenever there is
     * data to send (publishes time out), so the default favours battery.
     */
    val keepAliveSeconds: Int = 60,
    val qos: PublishQos = PublishQos.AT_LEAST_ONCE,
    val topics: SpotflowTopics = SpotflowTopics(),
    /**
     * Max size of the store-and-forward buffer for the whole gateway — all devices together — in bytes
     * (RAM tier + flash tier). Messages received over BLE are buffered here and drained to MQTT when the
     * network is available. When it is full, the device holding the most data loses its oldest messages
     * first, so one flooding or long-offline device can't push out the others. Default 50 MiB. If this is
     * not larger than [ramBufferMaxBytes] there is no flash tier: the buffer is RAM-only and never writes
     * to flash.
     *
     * Sizes count each message's payload plus a fixed estimate of what holding it costs (object headers
     * in RAM; row and index overhead in flash), so the limit tracks real use even for tiny messages.
     */
    val bufferMaxBytes: Long = 50L * 1024 * 1024,

    /**
     * Size of the in-memory (RAM) tier for the whole gateway, in bytes. In steady-state (online) operation
     * data flows through RAM only, so flash is not written; the oldest messages of the device holding the
     * most RAM spill to the flash tier only once RAM fills up (i.e. connectivity has been down long enough
     * to accumulate a backlog). The trade-off is that data still in RAM is lost if the process is killed.
     * Default 1 MiB.
     */
    val ramBufferMaxBytes: Long = 1L * 1024 * 1024,
)

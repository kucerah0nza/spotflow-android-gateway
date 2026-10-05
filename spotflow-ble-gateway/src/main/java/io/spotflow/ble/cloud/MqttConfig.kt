package io.spotflow.ble.cloud

import com.hivemq.client.mqtt.datatypes.MqttQos

/**
 * MQTT topic names on the Spotflow broker. Kept configurable because whether these are literal or
 * device-ID-templated is still being confirmed with the Spotflow backend.
 */
data class SpotflowTopics(
    val ingest: String = "ingest-cbor",
    val reportedConfiguration: String = "config-cbor-d2c",
    val desiredConfiguration: String = "config-cbor-c2d",
)

/**
 * Connection settings for the Spotflow MQTT broker. Defaults target production: `mqtt.spotflow.io:8883`
 * over TLS (Let's Encrypt ISRG Root X1, validated by the Android system trust store) with QoS 1.
 */
data class MqttConfig(
    val host: String = "mqtt.spotflow.io",
    val port: Int = 8883,
    val useTls: Boolean = true,
    val keepAliveSeconds: Int = 30,
    val qos: MqttQos = MqttQos.AT_LEAST_ONCE,
    val topics: SpotflowTopics = SpotflowTopics(),
    /**
     * Max size of the store-and-forward buffer for the whole gateway — all devices together — in bytes
     * (RAM tier + flash tier). Messages received over BLE are buffered here and drained to MQTT when the
     * network is available. When it is full, the device holding the most data loses its oldest messages
     * first, so one flooding or long-offline device can't push out the others. Default 50 MiB. If this is
     * not larger than [ramBufferMaxBytes] there is no flash tier: the buffer is RAM-only and never writes
     * to flash.
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

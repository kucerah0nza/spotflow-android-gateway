package io.spotflow.ble.cloud

import com.hivemq.client.mqtt.datatypes.MqttQos

/**
 * MQTT topic names on the Spotflow broker. Kept configurable because whether these are literal or
 * device-ID-templated is still being confirmed with the Spotflow backend (see README "Open items").
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
     * Max total size of the store-and-forward buffer per device, in bytes (RAM tier + disk tier).
     * Messages received over BLE are buffered here and drained to MQTT when the network is available;
     * when the buffer is full the oldest messages are evicted. Default 50 MiB.
     */
    val bufferMaxBytes: Long = 50L * 1024 * 1024,

    /**
     * Size of the in-memory (RAM) tier of the buffer, in bytes. In steady-state (online) operation data
     * flows through RAM only, so flash is not written; the buffer spills the oldest messages to the disk
     * tier only once RAM fills up (i.e. connectivity has been down long enough to accumulate a backlog).
     * The trade-off is that data still in RAM is lost if the process is killed. Default 2 MiB.
     */
    val ramBufferMaxBytes: Long = 2L * 1024 * 1024,
)

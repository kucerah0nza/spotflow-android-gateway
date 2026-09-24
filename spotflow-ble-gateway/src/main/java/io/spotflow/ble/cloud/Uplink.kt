package io.spotflow.ble.cloud

/**
 * The cloud uplink used by the gateway's drainer. Abstracted so [MqttUplink] can be swapped for a fake
 * in orchestration tests.
 */
interface Uplink {
    /** Whether the uplink currently has a live connection. */
    val isConnected: Boolean

    /**
     * Invoked when desired configuration arrives from the cloud (to be written back to the device). The
     * handler must call `ack` once the payload has been handled; an unacknowledged message is redelivered
     * by the broker. If no handler is set, messages are left unacknowledged.
     */
    var desiredConfigurationHandler: ((payload: ByteArray, ack: () -> Unit) -> Unit)?

    /** Connects (and subscribes). Throws [MqttAuthException] for a rejected key, else a retryable error. */
    suspend fun connect()

    /** Publishes one payload to [topic], suspending until acknowledged. */
    suspend fun publish(topic: String, payload: ByteArray)

    /** Disconnects and releases the connection. */
    suspend fun disconnect()
}

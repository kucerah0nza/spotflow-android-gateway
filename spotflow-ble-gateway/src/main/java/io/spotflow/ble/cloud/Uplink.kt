package io.spotflow.ble.cloud

/**
 * The cloud uplink used by a device's drain loop. Abstracted so [MqttUplink] can be swapped for a fake in
 * orchestration tests; `MqttUplinkBrokerTest` holds the real client to the behaviour described here.
 */
internal interface Uplink {
    /** Whether the uplink has a live connection that is ready for [publish] (connected *and* subscribed). */
    val isConnected: Boolean

    /**
     * Invoked when desired configuration arrives from the cloud (to be written back to the device). The
     * handler must call `ack` once the payload has been handled; an unacknowledged message is redelivered
     * by the broker. If no handler is set, messages are left unacknowledged.
     */
    var desiredConfigurationHandler: ((payload: ByteArray, ack: () -> Unit) -> Unit)?

    /** Invoked when an OTA command arrives, like [desiredConfigurationHandler]. */
    var otaCommandHandler: ((payload: ByteArray, ack: () -> Unit) -> Unit)?

    /** Invoked, on any thread, when an established connection is lost (not after [disconnect]). */
    var onConnectionLost: (() -> Unit)?

    /** Whether OTA commands are subscribed to on the current connection; false after every [connect]. */
    val isOtaSubscribed: Boolean

    /**
     * Whether a subscription to OTA commands may still be held by the broker for this device: one made on
     * an earlier connection outlives it (persistent session), until [unsubscribeOta].
     */
    val mayHoldOtaSubscription: Boolean

    /**
     * Subscribes to OTA commands on the current connection; the cloud then sends an unfinished update
     * again. Throws if the connection is lost meanwhile or the broker refuses.
     */
    suspend fun subscribeOta()

    /** Ends the subscription to OTA commands. Throws like [subscribeOta]. */
    suspend fun unsubscribeOta()

    /**
     * Connects and subscribes. Throws [MqttAuthException] for a rejected key, else a retryable error.
     * Cancelling it leaves no connection behind.
     */
    suspend fun connect()

    /**
     * Publishes one payload to [topic], suspending until the broker acknowledges it. Several publishes may
     * be in flight at once; they reach the broker in the order they were started.
     *
     * Throws [UplinkDisconnectedException] as soon as the connection is lost, [UplinkThrottledException] if
     * the broker is over quota, or whatever the broker answered if it refused the message itself. A
     * publish that did not complete is never retried by the uplink itself: the caller owns retransmission.
     */
    suspend fun publish(topic: String, payload: ByteArray)

    /** Disconnects and releases the connection. */
    suspend fun disconnect()
}

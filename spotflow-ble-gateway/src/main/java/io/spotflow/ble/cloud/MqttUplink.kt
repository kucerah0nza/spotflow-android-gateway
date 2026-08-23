package io.spotflow.ble.cloud

import android.util.Log
import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * One MQTT-over-TLS connection to the Spotflow cloud for a single device.
 *
 * Authentication follows the Spotflow ingest model: MQTT username = [deviceId], MQTT password = the
 * ingest key from [credentials]. Reassembled BLE messages are published as CBOR payloads; desired
 * configuration arriving from the cloud is delivered to [desiredConfigurationHandler].
 *
 * TLS uses the Android system trust store (Let's Encrypt ISRG Root X1), so no CA is bundled. This client
 * does not reconnect on its own — the gateway's drainer owns the connect/reconnect lifecycle so it can
 * keep buffering to disk while offline and flush when the network returns.
 */
class MqttUplink(
    private val deviceId: String,
    private val credentials: CredentialsProvider,
    private val config: MqttConfig = MqttConfig(),
) : Uplink {
    /** Invoked (off the caller's thread) when a DESIRED_CONFIGURATION payload arrives from the cloud. */
    @Volatile
    override var desiredConfigurationHandler: ((ByteArray) -> Unit)? = null

    private val client: Mqtt5AsyncClient = buildClient()

    override val isConnected: Boolean get() = client.state.isConnected

    private fun buildClient(): Mqtt5AsyncClient {
        // No automatic reconnect: the drainer owns the connect/reconnect lifecycle so it can keep
        // buffering to disk while offline and flush when the network returns.
        var builder = MqttClient.builder()
            .useMqttVersion5()
            .identifier(deviceId)
            .serverHost(config.host)
            .serverPort(config.port)
            .addConnectedListener { Log.i(TAG, "MQTT connected for $deviceId") }
            .addDisconnectedListener { Log.w(TAG, "MQTT disconnected for $deviceId: ${it.cause.message}") }
        if (config.useTls) {
            builder = builder.sslWithDefaultConfig()
        }
        return builder.buildAsync()
    }

    /**
     * Connects and subscribes to the desired-configuration topic. Throws [MqttAuthException] if the
     * broker rejects the credentials, or [MqttConnectException] for other connect failures.
     */
    override suspend fun connect() {
        val ingestKey = credentials.ingestKey(deviceId)
        try {
            client.connectWith()
                .simpleAuth()
                .username(deviceId)
                .password(ingestKey.toByteArray(Charsets.UTF_8))
                .applySimpleAuth()
                .keepAlive(config.keepAliveSeconds)
                .cleanStart(false)
                .sessionExpiryInterval(SESSION_EXPIRY_SECONDS)
                .send()
                .await()

            // Subscribe inside the same try: a subscribe failure must not leave the client connected
            // but unsubscribed (which would silently break the desired-config downlink).
            client.subscribeWith()
                .topicFilter(config.topics.desiredConfiguration)
                .qos(config.qos)
                .callback(::onCloudMessage)
                .send()
                .await()
        } catch (t: Throwable) {
            // Ensure we never leave a half-established connection; force a clean reconnect next time.
            runCatching { client.disconnect().await() }
            throw translateConnectError(t)
        }
    }

    /** Publishes one payload to [topic] at the configured QoS, suspending until acknowledged. */
    override suspend fun publish(topic: String, payload: ByteArray) {
        client.publishWith()
            .topic(topic)
            .qos(config.qos)
            .payload(payload)
            .send()
            .await()
    }

    override suspend fun disconnect() {
        runCatching { client.disconnect().await() }
    }

    private fun onCloudMessage(publish: Mqtt5Publish) {
        desiredConfigurationHandler?.invoke(publish.payloadAsBytes)
    }

    /** Maps a HiveMQ connect failure to a clear, shareable exception carrying the broker's reason. */
    private fun translateConnectError(error: Throwable): Exception {
        val connAck = generateSequence(error) { it.cause }
            .filterIsInstance<Mqtt5ConnAckException>()
            .firstOrNull()
            ?: return MqttConnectException(error.message ?: "MQTT connection failed", error)

        val ack = connAck.mqttMessage
        val reasonCode = ack.reasonCode
        val reasonString = ack.reasonString.map { it.toString() }.orElse(null)
        val detail = buildString {
            append(reasonCode.name)
            if (!reasonString.isNullOrBlank()) append(": ").append(reasonString)
        }

        return when (reasonCode) {
            Mqtt5ConnAckReasonCode.BAD_USER_NAME_OR_PASSWORD,
            Mqtt5ConnAckReasonCode.NOT_AUTHORIZED,
            Mqtt5ConnAckReasonCode.BANNED,
            ->
                MqttAuthException("Broker rejected the ingest key ($detail)")
            else ->
                MqttConnectException("Broker refused the connection ($detail)", error)
        }
    }

    companion object {
        private const val TAG = "SpotflowMqtt"
        private const val SESSION_EXPIRY_SECONDS = 3600L
    }
}

/** Bridges a HiveMQ [CompletableFuture] to a cancellable coroutine. */
private suspend fun <T> CompletableFuture<T>.await(): T = suspendCancellableCoroutine { cont ->
    whenComplete { value, error ->
        if (error != null) cont.resumeWithException(error) else cont.resume(value)
    }
    cont.invokeOnCancellation { cancel(true) }
}

package io.spotflow.ble.cloud

import android.util.Log
import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
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
 * TLS uses the Android system trust store (Let's Encrypt ISRG Root X1), so no CA is bundled. The client
 * reconnects automatically and buffers outgoing publishes while briefly disconnected.
 */
class MqttUplink(
    private val deviceId: String,
    private val credentials: CredentialsProvider,
    private val config: MqttConfig = MqttConfig(),
) {
    /** Invoked (off the caller's thread) when a DESIRED_CONFIGURATION payload arrives from the cloud. */
    @Volatile
    var desiredConfigurationHandler: ((ByteArray) -> Unit)? = null

    private val client: Mqtt5AsyncClient = buildClient()

    val isConnected: Boolean get() = client.state.isConnected

    private fun buildClient(): Mqtt5AsyncClient {
        var builder = MqttClient.builder()
            .useMqttVersion5()
            .identifier(deviceId)
            .serverHost(config.host)
            .serverPort(config.port)
            .automaticReconnectWithDefaultConfig()
            .addConnectedListener { Log.i(TAG, "MQTT connected for $deviceId") }
            .addDisconnectedListener { Log.w(TAG, "MQTT disconnected for $deviceId: ${it.cause.message}") }
        if (config.useTls) {
            builder = builder.sslWithDefaultConfig()
        }
        return builder.buildAsync()
    }

    /** Connects and subscribes to the desired-configuration topic. */
    suspend fun connect() {
        val ingestKey = credentials.ingestKey(deviceId)
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

        client.subscribeWith()
            .topicFilter(config.topics.desiredConfiguration)
            .qos(config.qos)
            .callback(::onCloudMessage)
            .send()
            .await()
    }

    suspend fun publishTelemetry(payload: ByteArray) = publish(config.topics.ingest, payload)

    suspend fun publishSessionMetadata(payload: ByteArray) = publish(config.topics.ingest, payload)

    suspend fun publishReportedConfiguration(payload: ByteArray) =
        publish(config.topics.reportedConfiguration, payload)

    private suspend fun publish(topic: String, payload: ByteArray) {
        client.publishWith()
            .topic(topic)
            .qos(config.qos)
            .payload(payload)
            .send()
            .await()
    }

    suspend fun disconnect() {
        runCatching { client.disconnect().await() }
    }

    private fun onCloudMessage(publish: Mqtt5Publish) {
        desiredConfigurationHandler?.invoke(publish.payloadAsBytes)
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

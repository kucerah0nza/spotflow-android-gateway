package io.spotflow.ble.cloud

import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.MqttClientState
import com.hivemq.client.mqtt.MqttGlobalPublishFilter
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.lifecycle.MqttClientDisconnectedContext
import com.hivemq.client.mqtt.lifecycle.MqttDisconnectSource
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5DisconnectException
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5DisconnectReasonCode
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode
import io.spotflow.ble.Slog
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * One MQTT-over-TLS connection to the Spotflow cloud for a single device.
 *
 * Authentication follows the Spotflow ingest model: MQTT username = [deviceId], MQTT password = the
 * ingest key from [credentials]. The MQTT client identifier is also [deviceId], so the broker allows only
 * one connection per device; the gateway keeps at most one uplink per device ID alive.
 *
 * Delivery is at-least-once (QoS 1 with a persistent session): when a connection is lost, the publishes
 * still waiting for their acknowledgement may or may not have reached the broker, and the gateway sends
 * them again — so the backend must tolerate duplicates. Reassembled BLE messages are published as CBOR
 * payloads; desired configuration and OTA commands arriving from the cloud are delivered to
 * [desiredConfigurationHandler] and [otaCommandHandler].
 *
 * TLS uses the Android system trust store (Let's Encrypt ISRG Root X1), so no CA is bundled. This client
 * does not reconnect on its own — the gateway's drainer owns the connect/reconnect lifecycle so it can
 * keep buffering while offline and flush when the network returns.
 *
 * ## Why the underlying client is sometimes replaced
 * The HiveMQ client keeps going after its caller has given up, in two ways that the drainer must not be
 * exposed to (both are pinned by `MqttUplinkBrokerTest`):
 *  - a connect whose coroutine was cancelled still completes, leaving a connected client that never
 *    subscribed;
 *  - a publish that was in flight when the connection dropped stays in the client's session and is sent
 *    again by the client after the next connect — in addition to the drainer's own retry, and even if the
 *    drainer has decided to drop that message.
 * A client in either state is therefore discarded (disconnected as soon as it can be) and a fresh one is
 * used for the next connection.
 */
internal class MqttUplink(
    private val deviceId: String,
    private val credentials: CredentialsProvider,
    private val config: MqttConfig = MqttConfig(),
) : Uplink {
    /**
     * Invoked (off the caller's thread) when a DESIRED_CONFIGURATION payload arrives from the cloud. The
     * subscription uses manual acknowledgement, so the broker only considers a configuration delivered once
     * the handler calls `ack` (i.e. after it was written to the device).
     */
    @Volatile
    override var desiredConfigurationHandler: ((ByteArray, () -> Unit) -> Unit)? = null

    @Volatile
    override var otaCommandHandler: ((ByteArray, () -> Unit) -> Unit)? = null

    @Volatile
    override var onConnectionLost: (() -> Unit)? = null

    override val isOtaSubscribed: Boolean get() = session.let { it.isReady && it.otaSubscribed }

    @Volatile
    override var mayHoldOtaSubscription: Boolean = false
        private set

    override suspend fun subscribeOta() {
        val current = readySession()
        // Before the SUBACK: the broker may hold the subscription even if the answer never arrives.
        mayHoldOtaSubscription = true
        current.subscribe(config.topics.otaCommands)
        current.otaSubscribed = true
    }

    override suspend fun unsubscribeOta() {
        val current = readySession()
        current.unsubscribe(config.topics.otaCommands)
        current.otaSubscribed = false
        mayHoldOtaSubscription = false
    }

    private fun readySession(): ClientSession =
        session.takeIf { it.isReady } ?: throw UplinkDisconnectedException("MQTT is not connected", rejectedByBroker = false)

    private val qos: MqttQos = when (config.qos) {
        PublishQos.AT_MOST_ONCE -> MqttQos.AT_MOST_ONCE
        PublishQos.AT_LEAST_ONCE -> MqttQos.AT_LEAST_ONCE
        PublishQos.EXACTLY_ONCE -> MqttQos.EXACTLY_ONCE
    }

    @Volatile
    private var session = ClientSession()

    override val isConnected: Boolean get() = session.isReady

    /**
     * Connects and subscribes to the desired-configuration topic. Throws [MqttAuthException] if the
     * broker rejects the credentials, or [MqttConnectException] for other connect failures.
     */
    override suspend fun connect() {
        val ingestKey = credentials.ingestKey(deviceId)
        var current = session
        if (!current.isReusable) {
            current.discard()
            current = ClientSession().also { session = it }
        }
        try {
            current.connect(ingestKey)
        } catch (c: CancellationException) {
            // The attempt carries on inside the client; make sure it doesn't end up as a live connection.
            current.discard()
            throw c
        } catch (t: Throwable) {
            // Never leave a half-established connection (e.g. connected but not subscribed).
            current.discard()
            throw translateConnectError(t)
        }
    }

    /** Publishes one payload to [topic] at the configured QoS, suspending until acknowledged. */
    override suspend fun publish(topic: String, payload: ByteArray) = session.publish(topic, payload)

    override suspend fun disconnect() {
        val current = session
        current.ready = false // a deliberate disconnect is not a lost connection
        runCatching { current.client.disconnect().await() }
        current.discard() // covers a connect that is still in flight
    }

    /** One HiveMQ client, and what is known about its current connection. */
    private inner class ClientSession {
        val client: Mqtt5AsyncClient = buildClient()

        /** Connected and subscribed. */
        @Volatile var ready = false

        /** Subscribed to OTA commands on the current connection. */
        @Volatile var otaSubscribed = false

        /** A publish was abandoned while in flight: the client would re-send it on its own. */
        @Volatile var tainted = false

        /** Completes when the current connection ends; `true` if the broker ended it in protest. */
        @Volatile var lost = CompletableDeferred<Boolean>()

        @Volatile private var connecting: CompletableFuture<*>? = null

        val isReady: Boolean get() = ready && client.state.isConnected

        /** Whether this client can be used for another connection. */
        val isReusable: Boolean get() = !tainted && client.state == MqttClientState.DISCONNECTED

        private fun buildClient(): Mqtt5AsyncClient {
            // No automatic reconnect: the drainer owns the connect/reconnect lifecycle.
            var builder = MqttClient.builder()
                .useMqttVersion5()
                .identifier(deviceId)
                .serverHost(config.host)
                .serverPort(config.port)
                .addConnectedListener { Slog.i(TAG, "MQTT connected for $deviceId") }
                .addDisconnectedListener(::onDisconnected)
            if (config.useTls) {
                builder = builder.sslWithDefaultConfig()
            }
            return builder.buildAsync().also { client ->
                // Registered before connecting and for ALL messages, with manual acknowledgement: the broker
                // redelivers queued desired configuration right after CONNACK — before our SUBACK — and on
                // a per-device topic that doesn't match our filter under MQTT rules (see onCloudMessage).
                client.publishes(MqttGlobalPublishFilter.ALL, ::onCloudMessage, true)
            }
        }

        suspend fun connect(ingestKey: String) {
            ready = false
            otaSubscribed = false
            lost = CompletableDeferred()
            val pending = client.connectWith()
                .simpleAuth()
                .username(deviceId)
                .password(ingestKey.toByteArray(Charsets.UTF_8))
                .applySimpleAuth()
                .keepAlive(config.keepAliveSeconds)
                .cleanStart(false)
                .sessionExpiryInterval(SESSION_EXPIRY_SECONDS)
                .send()
            connecting = pending
            // Not cancelled with the coroutine: cancelling the future would not stop the attempt, only hide
            // when it ends — and discard() needs to know that.
            pending.await(cancelFuture = false)

            subscribe(config.topics.desiredConfiguration)
            ready = true
        }

        /**
         * Subscribes to [topic] at the configured QoS. Throws [MqttConnectException] if the connection is
         * lost meanwhile or the broker refuses.
         */
        suspend fun subscribe(topic: String) {
            // No per-subscription callback: messages are handled by the global handler registered in
            // buildClient() (see onCloudMessage for why).
            val subAck = raceAgainstConnection("subscribing to '$topic'") {
                client.subscribeWith().topicFilter(topic).qos(qos).send()
            }
            // The broker answers per topic filter; a refusal still completes the SUBACK normally.
            val reasons = subAck.reasonCodes
            Slog.d(TAG, "subscribed $deviceId to '$topic': $reasons")
            if (reasons.any { it.isError }) throw MqttConnectException("Broker refused the subscription to '$topic' ($reasons)")
        }

        suspend fun unsubscribe(topic: String) {
            val unsubAck = raceAgainstConnection("unsubscribing from '$topic'") {
                client.unsubscribeWith().topicFilter(topic).send()
            }
            Slog.d(TAG, "unsubscribed $deviceId from '$topic': ${unsubAck.reasonCodes}")
        }

        /**
         * Awaits [request], but fails as soon as the connection is lost: with a persistent session the client
         * does not fail a pending (un)subscribe when the connection drops — it would sit there until the
         * session expires, over an hour later, and the drain loop with it.
         */
        private suspend fun <T> raceAgainstConnection(what: String, request: () -> CompletableFuture<T>): T {
            val connectionLost = lost
            if (connectionLost.isCompleted) throw MqttConnectException("MQTT connection lost before $what")
            val result = CompletableDeferred<T>()
            request().whenComplete { value, error ->
                if (error != null) result.completeExceptionally(error) else result.complete(value)
            }
            return select {
                result.onAwait { it }
                connectionLost.onAwait { throw MqttConnectException("MQTT connection lost while $what") }
            }
        }

        @OptIn(ExperimentalCoroutinesApi::class)
        suspend fun publish(topic: String, payload: ByteArray) {
            if (!isReady) throw UplinkDisconnectedException("MQTT is not connected", rejectedByBroker = false)
            val connectionLost = lost
            val sending = client.publishWith()
                .topic(topic)
                .qos(qos)
                .payload(payload)
                .send()
            val acknowledged = CompletableDeferred<Unit>()
            sending.whenComplete { _, error ->
                if (error != null) acknowledged.completeExceptionally(error) else acknowledged.complete(Unit)
            }
            try {
                // With a persistent session the client does not fail a publish when the connection drops —
                // it would wait for the next connection. Race it against the connection instead.
                select<Unit> {
                    acknowledged.onAwait { }
                    connectionLost.onAwait { rejected ->
                        throw UplinkDisconnectedException("MQTT connection lost while publishing", rejected)
                    }
                }
            } catch (t: Throwable) {
                if (!sending.isDone) tainted = true
                if (t is Mqtt5PubAckException && t.mqttMessage.reasonCode == Mqtt5PubAckReasonCode.QUOTA_EXCEEDED) {
                    throw UplinkThrottledException("Broker refused the message: quota exceeded", t)
                }
                if (t is CancellationException || t is UplinkDisconnectedException || client.state.isConnected) throw t
                // The broker gave no answer for this message: the connection went away under it.
                val rejected = connectionLost.isCompleted && connectionLost.getCompleted()
                throw UplinkDisconnectedException(t.message ?: "MQTT connection lost while publishing", rejected, t)
            }
        }

        /**
         * Stops using this client and makes sure it ends up disconnected — right away if it is connected,
         * or as soon as a connect that is still in flight completes.
         */
        fun discard() {
            ready = false
            val pending = connecting
            if (pending != null && !pending.isDone) {
                pending.whenComplete { _, error -> if (error == null) runCatching { client.disconnect() } }
            } else if (client.state.isConnected) {
                runCatching { client.disconnect() }
            }
        }

        private fun onDisconnected(context: MqttClientDisconnectedContext) {
            val wasReady = ready
            ready = false
            val rejected = context.source == MqttDisconnectSource.SERVER && !isBrokersOwnReason(context.cause)
            lost.complete(rejected)
            Slog.w(TAG, "MQTT disconnected for $deviceId: ${context.cause.message}")
            if (wasReady && this === session) onConnectionLost?.invoke()
        }

        /**
         * The broker accepts a subscription to `config-cbor-c2d` but publishes desired configuration on a
         * per-device topic below it (seen: `config-cbor-c2d/<workspace>/<device>`), which that filter doesn't
         * match under MQTT rules. Like the Device SDK, anything under the desired-configuration topic is
         * treated as desired configuration.
         */
        private fun onCloudMessage(publish: Mqtt5Publish) {
            if (this !== session) return // a discarded client: leave it unacknowledged for the live one
            val topic = publish.topic.toString()
            val (kind, handler) = when {
                isTopicOrBelow(config.topics.desiredConfiguration, topic) -> "desired configuration" to desiredConfigurationHandler
                isTopicOrBelow(config.topics.otaCommands, topic) -> "OTA command" to otaCommandHandler
                else -> {
                    Slog.w(TAG, "ignoring message for $deviceId on unexpected topic '$topic'")
                    runCatching { publish.acknowledge() } // nothing to deliver; don't leave it pending
                    return
                }
            }
            Slog.d(TAG, "$kind for $deviceId on '$topic' (${publish.payloadAsBytes.size} bytes)")
            // No handler: leave it unacknowledged so the broker redelivers it rather than it being lost.
            handler ?: return
            handler(publish.payloadAsBytes) { runCatching { publish.acknowledge() } }
        }
    }

    /** Maps a HiveMQ connect failure to a clear, shareable exception carrying the broker's reason. */
    private fun translateConnectError(error: Throwable): Exception {
        if (error is MqttConnectException) return error
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
        /** Whether [topic] is [base] itself or any topic below it. */
        internal fun isTopicOrBelow(base: String, topic: String): Boolean =
            topic == base || topic.startsWith("$base/")

        /**
         * Whether the broker ended the connection for a reason of its own, as opposed to objecting to what
         * the client sent. A broker that closes the socket without saying why is not given that benefit.
         */
        private fun isBrokersOwnReason(cause: Throwable): Boolean {
            val reason = (cause as? Mqtt5DisconnectException)?.mqttMessage?.reasonCode ?: return false
            return reason in BROKER_SIDE_REASONS
        }

        private val BROKER_SIDE_REASONS = setOf(
            Mqtt5DisconnectReasonCode.NORMAL_DISCONNECTION,
            Mqtt5DisconnectReasonCode.SERVER_BUSY,
            Mqtt5DisconnectReasonCode.SERVER_SHUTTING_DOWN,
            Mqtt5DisconnectReasonCode.KEEP_ALIVE_TIMEOUT,
            Mqtt5DisconnectReasonCode.SESSION_TAKEN_OVER,
            Mqtt5DisconnectReasonCode.ADMINISTRATIVE_ACTION,
            Mqtt5DisconnectReasonCode.USE_ANOTHER_SERVER,
            Mqtt5DisconnectReasonCode.SERVER_MOVED,
            Mqtt5DisconnectReasonCode.CONNECTION_RATE_EXCEEDED,
            Mqtt5DisconnectReasonCode.MAXIMUM_CONNECT_TIME,
            // Rate and quota limits are about how much is sent, not about any one message.
            Mqtt5DisconnectReasonCode.MESSAGE_RATE_TOO_HIGH,
            Mqtt5DisconnectReasonCode.QUOTA_EXCEEDED,
            Mqtt5DisconnectReasonCode.RECEIVE_MAXIMUM_EXCEEDED,
        )

        private const val TAG = "SpotflowMqtt"
        private const val SESSION_EXPIRY_SECONDS = 3600L
    }
}

/**
 * Bridges a HiveMQ [CompletableFuture] to a cancellable coroutine. With [cancelFuture] off, cancelling the
 * coroutine leaves the future alone, so its eventual outcome can still be observed.
 */
private suspend fun <T> CompletableFuture<T>.await(cancelFuture: Boolean = true): T =
    suspendCancellableCoroutine { cont ->
        whenComplete { value, error ->
            if (error != null) cont.resumeWithException(error) else cont.resume(value)
        }
        if (cancelFuture) cont.invokeOnCancellation { cancel(true) }
    }

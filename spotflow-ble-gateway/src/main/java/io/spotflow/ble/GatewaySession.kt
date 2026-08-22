package io.spotflow.ble

import android.content.Context
import android.util.Log
import io.spotflow.ble.cloud.CredentialsProvider
import io.spotflow.ble.cloud.MqttAuthException
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.MqttUplink
import io.spotflow.ble.cloud.PersistentMessageQueue
import io.spotflow.ble.cloud.StoreAndForwardBuffer
import io.spotflow.ble.cloud.Uplink
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

/** Observable state of one device being gatewayed, surfaced to the host app / demo UI. */
data class GatewayDeviceState(
    val address: String,
    val deviceId: String? = null,
    val ble: ConnectionState = ConnectionState.DISCONNECTED,
    val cloudConnected: Boolean = false,
    val forwarded: Long = 0,
    val bufferedBytes: Long = 0,
    val error: String? = null,
)

/**
 * Bridges a single [BleConnection] to the Spotflow cloud with store-and-forward buffering.
 *
 * Received BLE messages are enqueued to an on-disk [PersistentMessageQueue] (independent of network
 * state), and a separate drainer publishes them to MQTT, owning the connect/reconnect lifecycle. This
 * way diagnostics keep buffering (bounded, evict-oldest) while the phone is offline and flush in order
 * when connectivity returns.
 */
internal class GatewaySession(
    private val context: Context,
    private val connection: BleConnection,
    private val credentials: CredentialsProvider,
    private val mqttConfig: MqttConfig,
    private val onStatus: (GatewayDeviceState) -> Unit,
    private val uplinkFactory: (deviceId: String) -> Uplink =
        { id -> MqttUplink(id, credentials, mqttConfig) },
) {
    /** Runs until the connection is torn down or the coroutine is cancelled. */
    suspend fun run() = coroutineScope {
        var status = GatewayDeviceState(connection.deviceAddress)
        fun push(update: (GatewayDeviceState) -> GatewayDeviceState) {
            status = update(status)
            onStatus(status)
        }

        val stateJob = launch {
            connection.state.collect { bleState -> push { it.copy(ble = bleState) } }
        }

        try {
            connection.prepare()
            val deviceId = connection.readDeviceId()
            push { it.copy(deviceId = deviceId) }

            val diskMaxBytes = (mqttConfig.bufferMaxBytes - mqttConfig.ramBufferMaxBytes).coerceAtLeast(0)
            val buffer = StoreAndForwardBuffer(
                PersistentMessageQueue(context, deviceId, diskMaxBytes),
                mqttConfig.ramBufferMaxBytes,
            )
            val uplink = uplinkFactory(deviceId)
            uplink.desiredConfigurationHandler = { payload ->
                launch { runCatching { connection.sendDesiredConfiguration(payload) } }
            }
            val drainSignal = Channel<Unit>(Channel.CONFLATED)

            try {
                runCatching {
                    buffer.enqueue(mqttConfig.topics.ingest, connection.readSessionMetadata())
                    push { it.copy(bufferedBytes = buffer.bytes) }
                    drainSignal.trySend(Unit)
                }

                // BLE -> buffer (RAM first; never blocks on the network).
                val pump = launch {
                    connection.incoming.collect { message ->
                        val topic = when (message.type) {
                            MessageType.TELEMETRY -> mqttConfig.topics.ingest
                            MessageType.REPORTED_CONFIGURATION -> mqttConfig.topics.reportedConfiguration
                            else -> return@collect
                        }
                        buffer.enqueue(topic, message.payload)
                        push { it.copy(bufferedBytes = buffer.bytes) }
                        drainSignal.trySend(Unit)
                    }
                }

                // Buffer -> MQTT, owning connect/reconnect so offline just keeps buffering.
                val drainer = launch { drain(uplink, buffer, drainSignal, ::push) }

                try {
                    connection.state.first {
                        it == ConnectionState.DISCONNECTED || it == ConnectionState.FAILED
                    }
                } finally {
                    pump.cancel()
                    drainer.cancel()
                }
            } finally {
                uplink.disconnect()
                buffer.close()
                push { it.copy(cloudConnected = false) }
            }
        } finally {
            stateJob.cancel()
            connection.close()
        }
    }

    private suspend fun drain(
        uplink: Uplink,
        buffer: StoreAndForwardBuffer,
        drainSignal: Channel<Unit>,
        push: ((GatewayDeviceState) -> GatewayDeviceState) -> Unit,
    ) {
        var backoff = INITIAL_BACKOFF_MS
        var attempts = 0
        while (true) {
            if (!uplink.isConnected) {
                try {
                    uplink.connect()
                    push { it.copy(cloudConnected = true, error = null) }
                    backoff = INITIAL_BACKOFF_MS
                } catch (c: CancellationException) {
                    throw c
                } catch (auth: MqttAuthException) {
                    throw auth // non-retryable: ends the session so the caller stops it
                } catch (t: Throwable) {
                    push { it.copy(cloudConnected = false, error = t.message) }
                    delay(backoff)
                    backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
                    continue
                }
            }

            val item = buffer.takeNext()
            if (item == null) {
                // Idle: wait for new data, but wake periodically to keep the MQTT link warm — so a
                // drop during a quiet period is reconnected proactively instead of on the next message.
                withTimeoutOrNull(IDLE_POLL_MS) { drainSignal.receiveCatching() }
                continue
            }

            try {
                uplink.publish(item.topic, item.payload)
                buffer.remove(item)
                attempts = 0
                backoff = INITIAL_BACKOFF_MS
                push { it.copy(forwarded = it.forwarded + 1, bufferedBytes = buffer.bytes) }
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                if (uplink.isConnected) {
                    // Connected but the publish failed: likely a poison message (e.g. too large).
                    // Retry a few times, then drop it so it can't block the whole buffer forever.
                    if (++attempts >= MAX_PUBLISH_ATTEMPTS) {
                        Log.w(TAG, "dropping message after $attempts failures: ${t.message}")
                        buffer.remove(item)
                        attempts = 0
                    } else {
                        buffer.requeue(item)
                    }
                } else {
                    buffer.requeue(item)
                    push { it.copy(cloudConnected = false) }
                }
                push { it.copy(bufferedBytes = buffer.bytes) }
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
            }
        }
    }

    private companion object {
        const val TAG = "SpotflowGateway"
        const val INITIAL_BACKOFF_MS = 1_000L
        const val MAX_BACKOFF_MS = 30_000L
        const val MAX_PUBLISH_ATTEMPTS = 5
        const val IDLE_POLL_MS = 15_000L
    }
}

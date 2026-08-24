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
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

/** Observable state of one device being gatewayed, surfaced to the host app / demo UI. */
data class GatewayDeviceState(
    val address: String,
    val deviceId: String? = null,
    val ble: ConnectionState = ConnectionState.DISCONNECTED,
    val cloudConnected: Boolean = false,
    val forwarded: Long = 0,
    /** Latest BLE signal strength in dBm (higher/closer to 0 is stronger), or null if not yet read. */
    val rssi: Int? = null,
    /** Bytes buffered in the in-memory tier (normal while briefly offline). */
    val ramBytes: Long = 0,
    /** Bytes spilled to the persistent (flash) tier (only after a longer outage). */
    val diskBytes: Long = 0,
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
        val statusLock = Any()
        // Guarded: the state mirror, pump, and drainer all push concurrently on a multi-threaded
        // dispatcher, so an unsynchronized read-modify-write would lose updates.
        fun push(update: (GatewayDeviceState) -> GatewayDeviceState) {
            synchronized(statusLock) {
                status = update(status)
                onStatus(status)
            }
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
                    push { it.copy(ramBytes = buffer.ramBytes, diskBytes = buffer.diskBytes) }
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
                        push { it.copy(ramBytes = buffer.ramBytes, diskBytes = buffer.diskBytes) }
                        drainSignal.trySend(Unit)
                    }
                }

                // Buffer -> MQTT, owning connect/reconnect so offline just keeps buffering.
                val drainer = launch { drain(uplink, buffer, drainSignal, ::push) }

                // Periodically sample the BLE signal strength for the UI.
                val rssiJob = launch {
                    while (true) {
                        runCatching { connection.readRssi() }.getOrNull()?.let { rssi ->
                            push { it.copy(rssi = rssi) }
                        }
                        delay(RSSI_INTERVAL_MS)
                    }
                }

                try {
                    connection.state.first {
                        it == ConnectionState.DISCONNECTED || it == ConnectionState.FAILED
                    }
                } finally {
                    pump.cancel()
                    drainer.cancel()
                    rssiJob.cancel()
                }
            } finally {
                // NonCancellable so this cleanup runs to completion even when the session is cancelled
                // (Stop / Bluetooth off) — otherwise the flush that preserves unsent data could be skipped.
                withContext(NonCancellable) {
                    uplink.disconnect()
                    buffer.flushToDisk() // preserve unsent data across the reconnect
                    push { it.copy(cloudConnected = false, ramBytes = buffer.ramBytes, diskBytes = buffer.diskBytes) }
                    buffer.close()
                }
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
                val published = withTimeoutOrNull(PUBLISH_TIMEOUT_MS) {
                    uplink.publish(item.topic, item.payload)
                    true
                } != null

                if (!published) {
                    // The publish stalled although the client still reports connected — typically a
                    // half-open connection on a flaky network. Force a reconnect so a hung publish can't
                    // block the buffer and make it fill up while the link looks connected.
                    Log.w(TAG, "publish stalled >${PUBLISH_TIMEOUT_MS}ms; forcing reconnect")
                    buffer.requeue(item)
                    runCatching { uplink.disconnect() }
                    push { it.copy(cloudConnected = false, ramBytes = buffer.ramBytes, diskBytes = buffer.diskBytes) }
                    continue
                }

                buffer.remove(item)
                attempts = 0
                backoff = INITIAL_BACKOFF_MS
                push { it.copy(forwarded = it.forwarded + 1, ramBytes = buffer.ramBytes, diskBytes = buffer.diskBytes) }
            } catch (c: CancellationException) {
                buffer.requeue(item) // don't lose the in-flight item on teardown; it gets flushed to disk
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
                push { it.copy(ramBytes = buffer.ramBytes, diskBytes = buffer.diskBytes) }
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
        const val PUBLISH_TIMEOUT_MS = 20_000L
        const val RSSI_INTERVAL_MS = 5_000L
    }
}

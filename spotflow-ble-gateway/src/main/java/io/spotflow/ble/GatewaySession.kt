package io.spotflow.ble

import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

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
 * Decides which devices the gateway relays. Called after the device ID has been read, before anything is
 * sent to the cloud under that ID.
 *
 * Managed mode connects to anything advertising the Spotflow service, and the BLE protocol does not
 * authenticate devices — so without a filter, any nearby peripheral could publish data (and receive
 * desired configuration) under any device ID in your workspace. Production integrations should accept
 * only the devices they expect (e.g. an allowlist, or IDs provisioned to the signed-in user).
 */
fun interface DeviceFilter {
    fun accept(address: String, deviceId: String): Boolean
}

/** Thrown when [DeviceFilter] rejects a device (or it reports an unusable device ID). Not retried. */
class DeviceRejectedException(message: String) : Exception(message)

/**
 * Bridges a single [BleConnection] to the device's [CloudLink].
 *
 * Received BLE messages are enqueued to the link's store-and-forward buffer (independent of network
 * state), and the link's drainer publishes them to MQTT, owning the connect/reconnect lifecycle. When
 * the BLE link drops, the session ends but the cloud link lingers (see [CloudLinkRegistry]) to finish
 * uploading what is buffered.
 */
internal class GatewaySession(
    private val connection: BleConnection,
    private val mqttConfig: MqttConfig,
    private val links: CloudLinkRegistry,
    private val onStatus: (GatewayDeviceState) -> Unit,
    private val deviceFilter: DeviceFilter? = null,
    /** Periodically read RSSI. Off for attached connections, whose GATT queue belongs to the host. */
    private val pollRssi: Boolean = true,
) {
    /** Runs until the connection is torn down or the coroutine is cancelled. */
    suspend fun run() = coroutineScope {
        var status = GatewayDeviceState(connection.deviceAddress)
        val statusLock = Any()
        // Guarded: the state mirror, pump, and drainer all push concurrently on a multi-threaded
        // dispatcher, so an unsynchronized read-modify-write would lose updates.
        val push: StatusPush = { update ->
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
            if (deviceId.isBlank()) throw DeviceRejectedException("device reported an empty device ID")
            push { it.copy(deviceId = deviceId) }
            if (deviceFilter?.accept(connection.deviceAddress, deviceId) == false) {
                throw DeviceRejectedException("device $deviceId rejected by the device filter")
            }

            val link = links.claim(deviceId, push)
            try {
                try {
                    link.enqueue(mqttConfig.topics.ingest, connection.readSessionMetadata())
                } catch (c: CancellationException) {
                    throw c
                } catch (_: Throwable) {
                    // Optional characteristic; relaying works without it.
                }

                // BLE -> buffer (RAM first; never blocks on the network).
                val pump = launch {
                    connection.incoming.collect { message ->
                        val topic = when (message.type) {
                            MessageType.TELEMETRY -> mqttConfig.topics.ingest
                            MessageType.REPORTED_CONFIGURATION -> mqttConfig.topics.reportedConfiguration
                            else -> return@collect
                        }
                        link.enqueue(topic, message.payload)
                    }
                }

                // Buffer -> MQTT, owning connect/reconnect so offline just keeps buffering.
                val drainer = launch { link.drain(stopWhenEmpty = false) }

                // Cloud -> device, in order, acknowledged to the broker only once written.
                val desired = launch {
                    link.deliverDesiredConfiguration { connection.sendDesiredConfiguration(it) }
                }

                // Periodically sample the BLE signal strength for the UI.
                val rssiJob = if (pollRssi) {
                    launch {
                        while (true) {
                            try {
                                val rssi = connection.readRssi()
                                push { it.copy(rssi = rssi) }
                            } catch (c: CancellationException) {
                                throw c
                            } catch (_: Throwable) {
                                // transient; try again next tick
                            }
                            delay(RSSI_INTERVAL_MS)
                        }
                    }
                } else {
                    null
                }

                try {
                    connection.state.first {
                        it == ConnectionState.DISCONNECTED || it == ConnectionState.FAILED
                    }
                } finally {
                    pump.cancel()
                    drainer.cancel()
                    desired.cancel()
                    rssiJob?.cancel()
                }
            } finally {
                // NonCancellable so the hand-off runs even when the session is cancelled (Stop /
                // Bluetooth off) — the link then lingers to upload (or at least persist) what's buffered.
                withContext(NonCancellable) { links.release(link) }
            }
        } finally {
            stateJob.cancel()
            withContext(NonCancellable) { connection.close() }
        }
    }

    private companion object {
        const val RSSI_INTERVAL_MS = 5_000L
    }
}

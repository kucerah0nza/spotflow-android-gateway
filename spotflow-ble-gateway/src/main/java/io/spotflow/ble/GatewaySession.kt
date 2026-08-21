package io.spotflow.ble

import io.spotflow.ble.cloud.CredentialsProvider
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.MqttUplink
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch

/** Observable state of one device being gatewayed, surfaced to the host app / demo UI. */
data class GatewayDeviceState(
    val address: String,
    val deviceId: String? = null,
    val ble: ConnectionState = ConnectionState.DISCONNECTED,
    val cloudConnected: Boolean = false,
    val forwarded: Long = 0,
    val error: String? = null,
)

/**
 * Bridges a single [BleConnection] to the Spotflow cloud: prepares the BLE link, reads the device ID,
 * opens the MQTT uplink, and runs the two relay pumps (BLE -> MQTT for telemetry/reported config, and
 * MQTT -> BLE for desired config) until cancelled or the link drops.
 */
internal class GatewaySession(
    private val connection: BleConnection,
    private val credentials: CredentialsProvider,
    private val mqttConfig: MqttConfig,
    private val onStatus: (GatewayDeviceState) -> Unit,
) {
    /** Runs until the connection is torn down or the coroutine is cancelled. */
    suspend fun run() = coroutineScope {
        var status = GatewayDeviceState(connection.deviceAddress)
        fun push(update: (GatewayDeviceState) -> GatewayDeviceState) {
            status = update(status)
            onStatus(status)
        }

        // Mirror BLE state to the UI for the whole session (CONNECTING / PREPARING / READY / ...).
        val stateJob = launch {
            connection.state.collect { bleState -> push { it.copy(ble = bleState) } }
        }

        connection.prepare()
        val deviceId = connection.readDeviceId()
        push { it.copy(deviceId = deviceId) }

        val uplink = MqttUplink(deviceId, credentials, mqttConfig)
        uplink.desiredConfigurationHandler = { payload ->
            launch { runCatching { connection.sendDesiredConfiguration(payload) } }
        }
        uplink.connect()
        push { it.copy(cloudConnected = true) }

        runCatching { uplink.publishSessionMetadata(connection.readSessionMetadata()) }

        // Relay TX Stream -> MQTT as a child job.
        val pump = launch {
            connection.incoming.collect { message ->
                when (message.type) {
                    MessageType.TELEMETRY -> uplink.publishTelemetry(message.payload)
                    MessageType.REPORTED_CONFIGURATION -> uplink.publishReportedConfiguration(message.payload)
                    else -> return@collect
                }
                push { it.copy(forwarded = it.forwarded + 1) }
            }
        }

        try {
            // Suspend until the link drops, then end the session so the caller (managed mode) tears
            // this down and reconnects. Returning normally on a drop keeps the reconnect prompt; a
            // failure during prepare/connect throws instead and is handled with backoff upstream.
            connection.state.first { it == ConnectionState.DISCONNECTED || it == ConnectionState.FAILED }
        } finally {
            stateJob.cancel()
            pump.cancel()
            uplink.disconnect()
            connection.close()
            push { it.copy(cloudConnected = false) }
        }
    }
}

package io.spotflow.ble

import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.protocol.DeviceCapabilities
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

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
    /**
     * Applies updates to this device's single status entry, shared with its [CloudLink] — so the buffer
     * and cloud state the link reports carry over across BLE reconnects instead of being reset.
     */
    private val status: StatusPush,
    private val deviceFilter: DeviceFilter = DeviceFilter.AcceptAll,
    /**
     * Whether anyone is looking at the signal strength right now; RSSI is read periodically while it is
     * true. `null` never reads it — for attached connections, whose GATT queue belongs to the host.
     */
    private val rssiWanted: Flow<Boolean>? = null,
) {
    /** Runs until the connection is torn down or the coroutine is cancelled. */
    suspend fun run() = coroutineScope {
        val push = status
        // A new link-level attempt: reset what describes the BLE link only. Buffer, cloud and delivery
        // figures belong to the device's cloud link, which may still be holding (and uploading) data.
        push { it.copy(rssi = null, error = null) }

        val stateJob = launch {
            connection.state.collect { bleState -> push { it.copy(ble = bleState) } }
        }

        try {
            // Order per the Spotflow BLE protocol: read Capabilities, Device ID and Session Metadata
            // first; enable the TX stream last.
            connection.prepare()
            try {
                val capabilities = connection.readCapabilities()
                if (!capabilities.isCompatible) {
                    Slog.w(TAG, "device speaks protocol $capabilities; this gateway speaks ${DeviceCapabilities.SUPPORTED_MAJOR}.x")
                } else if (capabilities.otaSupported) {
                    // The device then works without OTA: it updates over BLE only through a gateway that
                    // forwards its OTA messages, which this one does not do yet.
                    Slog.i(TAG, "device supports OTA updates (protocol $capabilities); they are not relayed by this gateway")
                }
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                Slog.w(TAG, "could not read capabilities: ${t.message}")
            }
            val deviceId = connection.readDeviceId()
            if (deviceId.isBlank()) throw DeviceRejectedException("device reported an empty device ID")
            push { it.copy(deviceId = deviceId) }
            if (!deviceFilter.accept(connection.deviceAddress, deviceId)) {
                throw DeviceRejectedException("device $deviceId rejected by the device filter")
            }

            val link = links.claim(deviceId, push)
            // Everything of this session that uses the link; see the hand-over below.
            val linkUsers = ArrayList<Job>()
            try {
                try {
                    link.enqueueSessionMetadata(mqttConfig.topics.ingest, connection.readSessionMetadata())
                } catch (c: CancellationException) {
                    throw c
                } catch (t: Throwable) {
                    // Optional characteristic; relaying works without it.
                    Slog.w(TAG, "session metadata unavailable for $deviceId: ${t.message}")
                }

                // Completed by the device's first reported configuration this session. The Device SDK
                // registers its desired-configuration handler just before sending that message; until
                // then it accepts RX writes but silently discards them.
                val deviceAcceptsConfiguration = CompletableDeferred<Unit>()

                // BLE -> buffer (RAM first; never blocks on the network).
                linkUsers += launch {
                    connection.incoming.collect { message ->
                        val topic = when (message.type) {
                            MessageType.TELEMETRY -> mqttConfig.topics.ingest
                            MessageType.REPORTED_CONFIGURATION -> {
                                if (deviceAcceptsConfiguration.complete(Unit)) {
                                    Slog.d(TAG, "$deviceId reported its configuration; desired configuration can be delivered")
                                }
                                mqttConfig.topics.reportedConfiguration
                            }
                            else -> return@collect
                        }
                        link.enqueue(topic, message.payload)
                    }
                }

                // Buffer -> MQTT, owning connect/reconnect so offline just keeps buffering.
                linkUsers += launch { link.drain(stopWhenEmpty = false) }

                // Cloud -> device, in order, acknowledged to the broker only once written — and only once
                // the device can take it (see above). A device that never reports configuration doesn't
                // support it, so pending desired configuration then stays queued rather than being lost.
                linkUsers += launch {
                    deviceAcceptsConfiguration.await()
                    link.deliverDesiredConfiguration { connection.sendDesiredConfiguration(it) }
                }

                // Reads are done: start the device's TX stream.
                connection.startStreaming()

                // Sample the BLE signal strength, but only while someone is watching it.
                val rssiJob = rssiWanted?.let { wanted ->
                    launch {
                        while (true) {
                            wanted.first { it }
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
                }

                try {
                    connection.state.first {
                        it == ConnectionState.DISCONNECTED || it == ConnectionState.FAILED
                    }
                } finally {
                    rssiJob?.cancel()
                }
            } finally {
                // NonCancellable so the hand-off runs even when the session is cancelled (Stop /
                // Bluetooth off) — the link then lingers to upload (or at least persist) what's buffered.
                withContext(NonCancellable) {
                    // The link is handed over only once this session has really let go of it: releasing
                    // it starts another drain loop, and a link has a single consumer. Cancelling without
                    // waiting would leave this session's loop running next to that one for a moment.
                    linkUsers.forEach { it.cancel() }
                    linkUsers.joinAll()
                    links.release(link)
                }
            }
        } finally {
            stateJob.cancel()
            withContext(NonCancellable) { connection.close() }
            // The state collector is gone by now, so say it here: whatever ended the session — a drop, an
            // error, a rejected key — the BLE link is no longer up.
            push { it.copy(ble = ConnectionState.DISCONNECTED, rssi = null) }
        }
    }

    private companion object {
        const val TAG = "SpotflowGateway"
        const val RSSI_INTERVAL_MS = 5_000L
    }
}

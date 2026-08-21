package io.spotflow.ble

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothManager
import android.content.Context
import android.util.Log
import androidx.annotation.RequiresPermission
import io.spotflow.ble.cloud.CredentialsProvider
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.transport.AttachedBleConnection
import io.spotflow.ble.transport.ManagedBleConnection
import io.spotflow.ble.transport.SpotflowGattSession
import io.spotflow.ble.transport.SpotflowScanner
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

/**
 * Top-level entry point of the Spotflow BLE gateway.
 *
 * Two ways to feed it devices:
 *  - **Managed** — call [startScanning] and the gateway discovers, connects, and relays every device
 *    advertising the Spotflow service, reconnecting with backoff.
 *  - **Attach** — call [attach] with a `BluetoothGatt` the host app already owns (see
 *    [AttachedBleConnection] for the forwarding contract).
 *
 * To keep relaying while the screen is off, run this from a foreground service (see
 * `SpotflowGatewayService`).
 */
class SpotflowGateway(
    private val context: Context,
    private val credentials: CredentialsProvider,
    private val mqttConfig: MqttConfig = MqttConfig(),
    private val requestedMtu: Int = SpotflowGattSession.MAX_MTU,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val jobs = ConcurrentHashMap<String, Job>()

    private val _devices = MutableStateFlow<Map<String, GatewayDeviceState>>(emptyMap())
    /** Live per-device status, keyed by Bluetooth address. */
    val devices: StateFlow<Map<String, GatewayDeviceState>> = _devices

    private val adapter get() =
        (context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager).adapter

    // ---- managed mode ----------------------------------------------------------------------------

    @RequiresPermission(allOf = [Manifest.permission.BLUETOOTH_SCAN, Manifest.permission.BLUETOOTH_CONNECT])
    fun startScanning() {
        if (jobs.containsKey(SCAN_JOB_KEY)) return
        jobs[SCAN_JOB_KEY] = scope.launch {
            SpotflowScanner(adapter).scan().collect { device ->
                if (!jobs.containsKey(device.address)) {
                    launchManaged(device.address) { autoConnect ->
                        ManagedBleConnection(context, device, requestedMtu, autoConnect)
                    }
                }
            }
        }
    }

    fun stopScanning() {
        jobs.remove(SCAN_JOB_KEY)?.cancel()
    }

    private fun launchManaged(
        address: String,
        connectionFactory: (autoConnect: Boolean) -> io.spotflow.ble.transport.BleConnection,
    ) {
        jobs[address] = scope.launch {
            var firstAttempt = true
            var backoff = INITIAL_BACKOFF_MS
            while (isActive) {
                // Direct connect on the first attempt (device is in range from the scan); use
                // autoConnect for reconnects so Android re-attaches whenever the device reappears.
                val connection = connectionFactory(!firstAttempt)
                try {
                    GatewaySession(connection, credentials, mqttConfig, ::updateStatus).run()
                    backoff = INITIAL_BACKOFF_MS // clean end; reset backoff before reconnect
                } catch (t: Throwable) {
                    Log.w(TAG, "session for $address failed: ${t.message}")
                    updateStatus(currentOf(address).copy(error = t.message, cloudConnected = false))
                }
                firstAttempt = false
                if (!isActive) break
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
            }
        }
    }

    // ---- attach mode -----------------------------------------------------------------------------

    /**
     * Attaches to a GATT the host app already owns. The host must forward its GATT callbacks to the
     * returned connection's `gattCallback` and must not issue competing GATT operations while active.
     * Attach sessions are single-shot: the host owns reconnection.
     */
    @SuppressLint("MissingPermission")
    fun attach(gatt: BluetoothGatt): AttachedBleConnection {
        val connection = AttachedBleConnection(gatt, requestedMtu)
        val address = connection.deviceAddress
        jobs[address] = scope.launch {
            try {
                GatewaySession(connection, credentials, mqttConfig, ::updateStatus).run()
            } catch (t: Throwable) {
                updateStatus(currentOf(address).copy(error = t.message, cloudConnected = false))
            }
        }
        return connection
    }

    /** Stops gatewaying a device (managed or attached) and cancels its session. */
    fun detach(address: String) {
        jobs.remove(address)?.cancel()
        _devices.update { it - address }
    }

    /** Stops everything and releases resources. */
    fun shutdown() {
        jobs.values.forEach { it.cancel() }
        jobs.clear()
        scope.cancel()
    }

    private fun updateStatus(state: GatewayDeviceState) {
        _devices.update { it + (state.address to state) }
    }

    private fun currentOf(address: String): GatewayDeviceState =
        _devices.value[address] ?: GatewayDeviceState(address)

    companion object {
        private const val TAG = "SpotflowGateway"
        private const val SCAN_JOB_KEY = "__scan__"
        private const val INITIAL_BACKOFF_MS = 2_000L
        private const val MAX_BACKOFF_MS = 30_000L
    }
}

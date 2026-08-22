package io.spotflow.ble

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.util.Log
import androidx.annotation.RequiresPermission
import androidx.core.content.ContextCompat
import io.spotflow.ble.cloud.CredentialsProvider
import io.spotflow.ble.cloud.MqttAuthException
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.transport.AttachedBleConnection
import io.spotflow.ble.transport.ConnectionState
import io.spotflow.ble.transport.ManagedBleConnection
import io.spotflow.ble.transport.SpotflowGattSession
import io.spotflow.ble.transport.SpotflowScanner
import kotlinx.coroutines.CancellationException
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

    private val adapter: android.bluetooth.BluetoothAdapter?
        get() = (context.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter

    /** True if the device has Bluetooth and it is currently turned on. */
    val isBluetoothEnabled: Boolean get() = adapter?.isEnabled == true

    private val appContext = context.applicationContext

    @Volatile private var wantScanning = false
    @Volatile private var receiverRegistered = false

    /**
     * Watches the Bluetooth adapter itself. Turning Bluetooth off often does not deliver a GATT
     * disconnect callback, which would otherwise leave sessions parked forever; observing the adapter
     * lets the gateway tear those sessions down and rediscover once Bluetooth returns — even with the
     * screen off / app backgrounded, since the gateway runs in a foreground service.
     */
    private val bluetoothStateReceiver = object : BroadcastReceiver() {
        @SuppressLint("MissingPermission")
        override fun onReceive(context: Context, intent: Intent) {
            if (intent.action != BluetoothAdapter.ACTION_STATE_CHANGED) return
            when (intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)) {
                BluetoothAdapter.STATE_TURNING_OFF, BluetoothAdapter.STATE_OFF -> handleBluetoothOff()
                BluetoothAdapter.STATE_ON -> handleBluetoothOn()
            }
        }
    }

    private fun handleBluetoothOff() {
        if (jobs.isEmpty()) return
        Log.w(TAG, "Bluetooth off; tearing down ${jobs.size} job(s)")
        jobs.values.forEach { it.cancel() }
        jobs.clear()
        _devices.update { devices ->
            devices.mapValues { it.value.copy(ble = ConnectionState.DISCONNECTED, cloudConnected = false) }
        }
    }

    @SuppressLint("MissingPermission")
    private fun handleBluetoothOn() {
        if (wantScanning) {
            Log.i(TAG, "Bluetooth on; resuming scanning")
            startScanning()
        }
    }

    private fun registerBluetoothReceiver() {
        if (receiverRegistered) return
        ContextCompat.registerReceiver(
            appContext,
            bluetoothStateReceiver,
            IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED),
            ContextCompat.RECEIVER_NOT_EXPORTED,
        )
        receiverRegistered = true
    }

    // ---- managed mode ----------------------------------------------------------------------------

    @RequiresPermission(allOf = [Manifest.permission.BLUETOOTH_SCAN, Manifest.permission.BLUETOOTH_CONNECT])
    fun startScanning() {
        wantScanning = true
        registerBluetoothReceiver()
        if (jobs.containsKey(SCAN_JOB_KEY)) return
        val bluetoothAdapter = adapter
        if (bluetoothAdapter == null || !bluetoothAdapter.isEnabled) {
            Log.w(TAG, "cannot scan: Bluetooth is ${if (bluetoothAdapter == null) "unavailable" else "off"}")
            return
        }
        jobs[SCAN_JOB_KEY] = scope.launch {
            try {
                SpotflowScanner(bluetoothAdapter).scan().collect { device ->
                    if (!jobs.containsKey(device.address)) {
                        launchManaged(device.address) { autoConnect ->
                            ManagedBleConnection(context, device, requestedMtu, autoConnect)
                        }
                    }
                }
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                // e.g. Bluetooth turned off mid-scan; end the scan cleanly instead of crashing.
                Log.w(TAG, "scanning stopped: ${t.message}")
            } finally {
                // Allow startScanning() to resume discovery later (e.g. once Bluetooth is back on).
                jobs.remove(SCAN_JOB_KEY)
            }
        }
    }

    fun stopScanning() {
        wantScanning = false
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
                    GatewaySession(context, connection, credentials, mqttConfig, ::updateStatus).run()
                    backoff = INITIAL_BACKOFF_MS // clean end; reset backoff before reconnect
                } catch (t: MqttAuthException) {
                    // The ingest key won't change until the gateway is restarted; stop retrying.
                    Log.w(TAG, "auth rejected for $address: ${t.message}")
                    updateStatus(currentOf(address).copy(error = t.message, cloudConnected = false))
                    break
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
                GatewaySession(context, connection, credentials, mqttConfig, ::updateStatus).run()
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
        wantScanning = false
        if (receiverRegistered) {
            runCatching { appContext.unregisterReceiver(bluetoothStateReceiver) }
            receiverRegistered = false
        }
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

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
import io.spotflow.ble.cloud.MqttUplink
import io.spotflow.ble.cloud.PersistentMessageQueue
import io.spotflow.ble.cloud.StoreAndForwardBuffer
import io.spotflow.ble.transport.AttachedBleConnection
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import io.spotflow.ble.transport.DeviceUnreachableException
import io.spotflow.ble.transport.ManagedBleConnection
import io.spotflow.ble.transport.SpotflowGattSession
import io.spotflow.ble.transport.SpotflowScanner
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

/**
 * Top-level entry point of the Spotflow BLE gateway.
 *
 * Two ways to feed it devices:
 *  - **Managed** — call [startScanning] and the gateway discovers, connects, and relays every device
 *    advertising the Spotflow service (that passes [deviceFilter]), reconnecting with backoff.
 *  - **Attach** — call [attach] with a `BluetoothGatt` the host app already owns (see
 *    [AttachedBleConnection] for the forwarding contract).
 *
 * Buffered data is uploaded whenever the phone is online — also after a device disconnects, and for
 * buffers left on disk by an earlier run (those are picked up when the gateway is created).
 *
 * To keep relaying while the screen is off, run this from a foreground service (see
 * `SpotflowGatewayService`).
 *
 * @param deviceFilter decides which devices are relayed; see [DeviceFilter] for why production
 *   integrations should set one. `null` accepts every device.
 */
class SpotflowGateway(
    context: Context,
    private val credentials: CredentialsProvider,
    private val mqttConfig: MqttConfig = MqttConfig(),
    private val requestedMtu: Int = SpotflowGattSession.MAX_MTU,
    private val deviceFilter: DeviceFilter? = null,
) {
    // Application context only: the gateway may outlive whatever component created it.
    private val appContext = context.applicationContext

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val jobs = ConcurrentHashMap<String, Job>()

    /** Addresses whose status is published; a status update for any other address is ignored. */
    private val tracked = ConcurrentHashMap.newKeySet<String>()

    private val links = CloudLinkRegistry(scope) { deviceId, push ->
        val diskMaxBytes = (mqttConfig.bufferMaxBytes - mqttConfig.ramBufferMaxBytes).coerceAtLeast(0)
        val disk = if (diskMaxBytes > 0) PersistentMessageQueue(appContext, deviceId, diskMaxBytes) else null
        CloudLink(
            deviceId,
            StoreAndForwardBuffer(disk, mqttConfig.ramBufferMaxBytes),
            MqttUplink(deviceId, credentials, mqttConfig),
            push,
        )
    }

    private val _devices = MutableStateFlow<Map<String, GatewayDeviceState>>(emptyMap())
    /** Live per-device status, keyed by Bluetooth address. */
    val devices: StateFlow<Map<String, GatewayDeviceState>> = _devices

    private val adapter: BluetoothAdapter?
        get() = (appContext.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter

    /** True if the device has Bluetooth and it is currently turned on. */
    val isBluetoothEnabled: Boolean get() = adapter?.isEnabled == true

    @Volatile private var wantScanning = false
    @Volatile private var receiverRegistered = false

    init {
        // Upload buffers left on disk by an earlier run (e.g. the app was killed during an outage) even if
        // those devices never come back.
        scope.launch {
            runCatching { PersistentMessageQueue.storedDeviceIds(appContext) }.getOrDefault(emptyList())
                .forEach { id -> runCatching { links.recover(id) } }
        }
    }

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
            devices.mapValues { it.value.copy(ble = ConnectionState.DISCONNECTED, rssi = null) }
        }
    }

    @SuppressLint("MissingPermission")
    private fun handleBluetoothOn() {
        if (wantScanning) {
            Log.i(TAG, "Bluetooth on; resuming scanning")
            startScanning()
        }
    }

    @Synchronized
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
        // LAZY: register the job before it runs, so its own cleanup can never race the registration.
        val job = scope.launch(start = CoroutineStart.LAZY) {
            try {
                SpotflowScanner(bluetoothAdapter).scan().collect { device ->
                    if (!jobs.containsKey(device.address)) {
                        launchManaged(device.address) { autoConnect ->
                            ManagedBleConnection(appContext, device, requestedMtu, autoConnect)
                        }
                    }
                }
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                // e.g. Bluetooth turned off mid-scan; end the scan cleanly instead of crashing.
                Log.w(TAG, "scanning stopped: ${t.message}")
            } finally {
                // Allow startScanning() to resume discovery later — but only remove *this* job, never a
                // newer scan started after a quick stop/start.
                jobs.remove(SCAN_JOB_KEY, coroutineContext.job)
            }
        }
        if (jobs.putIfAbsent(SCAN_JOB_KEY, job) == null) job.start() else job.cancel()
    }

    fun stopScanning() {
        wantScanning = false
        jobs.remove(SCAN_JOB_KEY)?.cancel()
    }

    private fun launchManaged(
        address: String,
        connectionFactory: (autoConnect: Boolean) -> BleConnection,
    ) {
        tracked += address
        val job = scope.launch(start = CoroutineStart.LAZY) {
            var firstAttempt = true
            var backoff = INITIAL_BACKOFF_MS
            while (isActive) {
                // Direct connect on the first attempt (device is in range from the scan); use
                // autoConnect for reconnects so Android re-attaches whenever the device reappears.
                val connection = connectionFactory(!firstAttempt)
                try {
                    GatewaySession(connection, mqttConfig, links, ::updateStatus, deviceFilter).run()
                    backoff = INITIAL_BACKOFF_MS // clean end; reset backoff before reconnect
                } catch (c: CancellationException) {
                    throw c // normal teardown (Stop / Bluetooth off / detach) — not an error
                } catch (t: MqttAuthException) {
                    // The ingest key won't change until the gateway is restarted; stop retrying.
                    Log.w(TAG, "auth rejected for $address: ${t.message}")
                    updateStatus(currentOf(address).copy(error = t.message, cloudConnected = false))
                    break
                } catch (t: DeviceRejectedException) {
                    // Not ours: stop, keep the job registered so the scanner doesn't reconnect it, and hide it.
                    Log.w(TAG, "ignoring $address: ${t.message}")
                    untrack(address)
                    break
                } catch (t: DeviceUnreachableException) {
                    // Gone for a long time: free the GATT slot. The scanner reconnects it if it reappears.
                    Log.i(TAG, "giving up on $address: ${t.message}")
                    untrack(address)
                    jobs.remove(address, coroutineContext.job)
                    break
                } catch (t: Throwable) {
                    Log.w(TAG, "session for $address failed: ${t.message}")
                    updateStatus(currentOf(address).copy(error = t.message))
                }
                firstAttempt = false
                if (!isActive) break
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
            }
        }
        jobs[address] = job
        job.start()
    }

    // ---- attach mode -----------------------------------------------------------------------------

    /**
     * Attaches to a GATT the host app already owns. The host must forward its GATT callbacks to the
     * returned connection's `gattCallback`, and must run any GATT operations of its own through
     * [AttachedBleConnection.runExclusive] while attached. Attach sessions are single-shot: the host
     * owns reconnection.
     *
     * @param pollRssi read RSSI periodically for [GatewayDeviceState.rssi]. Off by default, since it
     *   issues extra operations on the host's GATT.
     */
    @SuppressLint("MissingPermission")
    fun attach(gatt: BluetoothGatt, pollRssi: Boolean = false): AttachedBleConnection {
        registerBluetoothReceiver()
        val connection = AttachedBleConnection(gatt, requestedMtu)
        val address = connection.deviceAddress
        tracked += address
        val previous = jobs.remove(address)
        jobs[address] = scope.launch {
            // Let a previous session for this address release its cloud link first, or the new one
            // would be refused as a duplicate of the same device ID.
            previous?.cancelAndJoin()
            try {
                GatewaySession(connection, mqttConfig, links, ::updateStatus, deviceFilter, pollRssi).run()
            } catch (c: CancellationException) {
                throw c // normal teardown — not an error
            } catch (t: DeviceRejectedException) {
                Log.w(TAG, "ignoring $address: ${t.message}")
                untrack(address)
            } catch (t: Throwable) {
                updateStatus(currentOf(address).copy(error = t.message, cloudConnected = false))
            }
        }
        return connection
    }

    /**
     * Stops gatewaying a device (managed or attached) and cancels its session. Data it already buffered
     * is still uploaded in the background.
     */
    fun detach(address: String) {
        untrack(address)
        jobs.remove(address)?.cancel()
    }

    /** Stops everything and releases resources (unsent data is persisted where a flash tier exists). */
    fun shutdown() {
        wantScanning = false
        synchronized(this) {
            if (receiverRegistered) {
                runCatching { appContext.unregisterReceiver(bluetoothStateReceiver) }
                receiverRegistered = false
            }
        }
        jobs.values.forEach { it.cancel() }
        jobs.clear()
        scope.cancel()
    }

    private fun updateStatus(state: GatewayDeviceState) {
        // Checked inside update(): if a detach wins the race, the CAS retries and sees it untracked, so a
        // late update from a cancelled session can't resurrect a detached device.
        _devices.update { if (state.address in tracked) it + (state.address to state) else it }
    }

    private fun untrack(address: String) {
        tracked -= address
        _devices.update { it - address }
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

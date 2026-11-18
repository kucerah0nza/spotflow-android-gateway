package io.spotflow.ble

import java.io.File
import io.spotflow.ble.ota.ArtifactCache
import android.Manifest
import android.annotation.SuppressLint
import android.content.Context
import androidx.annotation.RequiresPermission
import io.spotflow.ble.cloud.BufferPool
import io.spotflow.ble.cloud.BufferUsage
import io.spotflow.ble.cloud.CredentialsProvider
import io.spotflow.ble.cloud.MessageStore
import io.spotflow.ble.cloud.MqttAuthException
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import io.spotflow.ble.transport.DeviceUnreachableException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap

/**
 * Top-level entry point of the Spotflow BLE gateway.
 *
 * Two ways to feed it devices:
 *  - **Managed** — call [startScanning] and the gateway discovers, connects, and relays every device
 *    advertising the Spotflow service that passes [deviceFilter], reconnecting with backoff.
 *  - **Attach** — call [newAttachment] for a `BluetoothGatt` the host app already owns (see
 *    [GattAttachment] for the contract).
 *
 * Buffered data is uploaded whenever the phone is online — also after a device disconnects, and for
 * buffers left on disk by an earlier run.
 *
 * To keep relaying while the screen is off, run this from a foreground service (see
 * `SpotflowGatewayService`). Creating a gateway is cheap and does no disk or network I/O on the calling
 * thread; call [shutdown] when done with it.
 */
public class SpotflowGateway internal constructor(
    context: Context,
    private val mqttConfig: MqttConfig,
    private val deviceFilter: DeviceFilter,
    private val requestedMtu: Int,
    private val platform: GatewayPlatform,
    dispatcher: CoroutineDispatcher,
    otaConfig: OtaConfig = OtaConfig(),
) {
    /**
     * @param deviceFilter decides which devices are relayed. It is required because the choice matters:
     *   the BLE protocol does not authenticate devices, so production integrations should accept only the
     *   devices they expect. Pass [DeviceFilter.AcceptAll] to relay everything in range.
     * @param requestedMtu the ATT MTU to ask for; the device may settle for less.
     * @param otaConfig over-the-air updates of devices that support them; on by default.
     */
    public constructor(
        context: Context,
        credentials: CredentialsProvider,
        deviceFilter: DeviceFilter,
        mqttConfig: MqttConfig = MqttConfig(),
        requestedMtu: Int = DEFAULT_MTU,
        otaConfig: OtaConfig = OtaConfig(),
    ) : this(
        context,
        mqttConfig,
        deviceFilter,
        requestedMtu,
        AndroidGatewayPlatform(context.applicationContext, credentials, mqttConfig, requestedMtu),
        Dispatchers.IO,
        otaConfig,
    )

    // Application context only: the gateway may outlive whatever component created it.
    private val appContext = context.applicationContext

    // The handler is a backstop: this runs inside someone else's app, and an exception that slipped
    // through must not take that app down (an uncaught one in a coroutine crashes the process).
    private val scope = CoroutineScope(
        SupervisorJob() + dispatcher + CoroutineExceptionHandler { _, error ->
            Slog.e(TAG, "unexpected failure in the gateway", error)
        },
    )
    private val jobs = ConcurrentHashMap<String, Job>()

    /** Addresses whose status is published; a status update for any other address is ignored. */
    private val tracked = ConcurrentHashMap.newKeySet<String>()

    /** One RAM and one flash budget shared by every device (see [BufferPool]). */
    private val buffers: BufferPool = run {
        val flashMaxBytes = (mqttConfig.bufferMaxBytes - mqttConfig.ramBufferMaxBytes).coerceAtLeast(0)
        val store = MessageStore.acquire(appContext, flashMaxBytes)
        BufferPool(store, mqttConfig.ramBufferMaxBytes, flashMaxBytes) { MessageStore.release(store) }
    }

    /** Live usage of the store-and-forward budget, across all devices (see [MqttConfig.bufferMaxBytes]). */
    public val bufferUsage: StateFlow<BufferUsage> get() = buffers.usage

    /** OTA artifacts downloaded for the devices, shared by all of them; null with OTA off. */
    private val artifacts: ArtifactCache? = otaConfig.takeIf { it.enabled }?.let {
        ArtifactCache({ File(appContext.cacheDir, "spotflow-ota") }, platform.artifactDownloader, it.maxArtifactBytes, scope)
    }

    private val links = CloudLinkRegistry(scope) { deviceId, push ->
        CloudLink(deviceId, buffers.open(deviceId), platform.createUplink(deviceId), push, platform.online)
    }

    private val _devices = MutableStateFlow<Map<String, GatewayDeviceState>>(emptyMap())

    /** Live per-device status, keyed by Bluetooth address. */
    public val devices: StateFlow<Map<String, GatewayDeviceState>> = _devices

    private val _status = MutableStateFlow(GatewayStatus())

    /** Gateway-wide status: whether discovery is running (or why not), connectivity, storage. */
    public val status: StateFlow<GatewayStatus> = _status

    /** True if the device has Bluetooth and it is currently turned on. */
    public val isBluetoothEnabled: Boolean get() = platform.isBluetoothEnabled

    /** Signal strength is only worth reading while someone is looking at [devices]. */
    private val rssiWanted: Flow<Boolean> = _devices.subscriptionCount.map { it > 0 }.distinctUntilChanged()

    @Volatile private var wantScanning = false

    /** Bumped by [retryStopped] to end the pauses of devices that were stopped. */
    private val retrySignal = MutableStateFlow(0)

    /** Managed devices the host took out with [detach]: the scan leaves them alone. */
    private val detached = ConcurrentHashMap.newKeySet<String>()

    init {
        platform.start(onBluetoothOff = ::handleBluetoothOff, onBluetoothOn = ::handleBluetoothOn)
        scope.launch { platform.online.collect { online -> _status.update { it.copy(online = online) } } }
        scope.launch { buffers.storeAvailable.collect { ok -> _status.update { it.copy(storageAvailable = ok) } } }
        // Upload buffers left on disk by an earlier run (e.g. the app was killed during an outage) even if
        // those devices never come back. This runs once right away — which is also what opens the flash
        // store, here rather than on the thread that created the gateway — and again whenever a gateway
        // that was shut down hands its data over: a restarted gateway overlaps with the previous one,
        // which may still be writing its RAM to flash after this one has looked.
        scope.launch { buffers.handoffs.collect { recoverStoredBuffers() } }
    }

    private fun recoverStoredBuffers() {
        val stored = runCatching { buffers.storedDeviceIds() }
            .onFailure { Slog.w(TAG, "cannot read the stored buffers: ${it.message}") }
            .getOrDefault(emptyList())
        stored.forEach { id -> runCatching { links.recover(id) } }
    }

    internal fun handleBluetoothOff() {
        if (wantScanning) setScan(ScanState.WAITING_FOR_BLUETOOTH)
        if (jobs.isEmpty()) return
        Slog.w(TAG, "Bluetooth off; tearing down ${jobs.size} job(s)")
        // The scan first, so it stops adding devices; then each job as it is taken out of the map.
        jobs.remove(SCAN_JOB_KEY)?.cancel()
        for (key in jobs.keys.toList()) jobs.remove(key)?.cancel()
        _devices.update { devices ->
            devices.mapValues { it.value.copy(ble = ConnectionState.DISCONNECTED, rssi = null) }
        }
    }

    @SuppressLint("MissingPermission") // held when scanning was first started
    internal fun handleBluetoothOn() {
        if (wantScanning) {
            Slog.i(TAG, "Bluetooth on; resuming scanning")
            startScanning()
        }
    }

    // ---- managed mode ----------------------------------------------------------------------------

    /**
     * Starts discovering and relaying devices. Scanning keeps itself going: a scan that fails is retried
     * with backoff, and it resumes when Bluetooth is turned back on. Follow it in [status].
     */
    @RequiresPermission(allOf = [Manifest.permission.BLUETOOTH_SCAN, Manifest.permission.BLUETOOTH_CONNECT])
    public fun startScanning() {
        wantScanning = true
        if (jobs.containsKey(SCAN_JOB_KEY)) return
        if (!platform.isBluetoothEnabled) {
            Slog.w(TAG, "cannot scan yet: Bluetooth is off or unavailable")
            setScan(ScanState.WAITING_FOR_BLUETOOTH)
            return
        }
        // LAZY: register the job before it runs, so its own cleanup can never race the registration.
        val job = scope.launch(start = CoroutineStart.LAZY) {
            try {
                scanLoop()
            } finally {
                // Allow startScanning() to resume discovery later — but only remove *this* job, never a
                // newer scan started after a quick stop/start.
                jobs.remove(SCAN_JOB_KEY, coroutineContext.job)
            }
        }
        if (jobs.putIfAbsent(SCAN_JOB_KEY, job) == null) job.start() else job.cancel()
    }

    /**
     * Scans until cancelled, in segments.
     *
     * The first segment is short and `BALANCED`, to find the devices in range quickly; from then on the
     * scan is low-power — known devices reconnect through `autoConnect`, so the scan only has to notice
     * new devices and those given up on. Each segment ends with a restart, because Android throttles a
     * scan left running for 30 minutes (down to results it only hears about from other apps' scans, on
     * older versions). A scan that fails — which Android reports once and never again — is retried with
     * backoff instead of leaving discovery dead until Bluetooth is toggled.
     */
    private suspend fun scanLoop() {
        var lowPower = false
        var backoff = SCAN_RETRY_INITIAL_MS
        while (currentCoroutineContext().isActive) {
            if (!platform.isBluetoothEnabled) {
                setScan(ScanState.WAITING_FOR_BLUETOOTH) // the adapter watcher restarts scanning
                return
            }
            try {
                setScan(ScanState.SCANNING)
                val segment = if (lowPower) SCAN_RESTART_MS else SCAN_BOOST_MS
                val ended = withTimeoutOrNull(segment) {
                    platform.scan(lowPower).collect { device ->
                        backoff = SCAN_RETRY_INITIAL_MS
                        discovered(device)
                    }
                }
                check(ended == null) { "the scan ended unexpectedly" }
                lowPower = true
                backoff = SCAN_RETRY_INITIAL_MS
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                // e.g. the scan could not be registered, a permission is missing, or Bluetooth went off.
                Slog.w(TAG, "scanning failed, retrying in ${backoff / 1000} s: ${t.message}")
                setScan(ScanState.RETRYING, t.message ?: t.javaClass.simpleName)
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(SCAN_RETRY_MAX_MS)
            }
        }
    }

    private fun setScan(state: ScanState, error: String? = null) {
        _status.update { it.copy(scan = state, scanError = error) }
    }

    /** Stops discovering new devices. Devices already being relayed stay connected. */
    public fun stopScanning() {
        wantScanning = false
        jobs.remove(SCAN_JOB_KEY)?.cancel()
        setScan(ScanState.STOPPED)
    }

    private fun discovered(device: DiscoveredDevice) {
        // Bluetooth may have just been turned off, with the teardown already past this point.
        if (!platform.isBluetoothEnabled || jobs.containsKey(device.address) || device.address in detached) return
        launchManaged(device)
    }

    private fun launchManaged(device: DiscoveredDevice) {
        val address = device.address
        tracked += address
        val job = scope.launch(start = CoroutineStart.LAZY) {
            // Direct connect while the device is likely in range — right after the scan found it, or right
            // after a working session dropped (usually a brief radio gap). Direct connects take about a
            // second; autoConnect scans at a low duty cycle and can take many seconds. Only after a direct
            // attempt fails does it fall back to autoConnect, which re-attaches whenever the device reappears.
            var direct = true
            var backoff = INITIAL_BACKOFF_MS
            var authBackoff = AUTH_RETRY_INITIAL_MS
            while (isActive) {
                val connection = device.connect(!direct)
                var cleanEnd = false
                try {
                    GatewaySession(connection, mqttConfig, links, statusOf(address), deviceFilter, rssiWanted, artifacts).run()
                    cleanEnd = true
                    backoff = INITIAL_BACKOFF_MS
                    authBackoff = AUTH_RETRY_INITIAL_MS
                } catch (c: CancellationException) {
                    throw c // normal teardown (Stop / Bluetooth off / detach) — not an error
                } catch (t: MqttAuthException) {
                    // Hammering the broker with a rejected key is pointless, but the key can change (a
                    // rotating CredentialsProvider, a key fixed on the server side): ask again rarely.
                    Slog.w(TAG, "auth rejected for $address, retrying in ${authBackoff / 1000} s: ${t.message}")
                    statusOf(address).invoke { it.copy(error = errorOf(t), cloudConnected = false) }
                    pause(authBackoff)
                    authBackoff = (authBackoff * 2).coerceAtMost(AUTH_RETRY_MAX_MS)
                    direct = true
                    continue
                } catch (t: DeviceRejectedException) {
                    // Not ours: hide it. The job stays registered for a while so the scanner doesn't
                    // reconnect it every time it advertises; after that it is looked at afresh, since
                    // what the filter accepts can change (another user signed in, a device provisioned).
                    Slog.w(TAG, "ignoring $address: ${t.message}")
                    untrack(address)
                    pause(REJECTED_RETRY_MS)
                    jobs.remove(address, coroutineContext.job)
                    return@launch
                } catch (t: DeviceUnreachableException) {
                    // Gone for a long time: free the GATT slot. The scanner reconnects it if it reappears.
                    Slog.i(TAG, "giving up on $address: ${t.message}")
                    untrack(address)
                    jobs.remove(address, coroutineContext.job)
                    return@launch
                } catch (t: Throwable) {
                    Slog.w(TAG, "session for $address failed: ${t.message}")
                    statusOf(address).invoke { it.copy(error = errorOf(t)) }
                }
                direct = cleanEnd
                if (!isActive) break
                if (cleanEnd) {
                    delay(RECONNECT_DELAY_MS)
                } else {
                    delay(backoff)
                    backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
                }
            }
        }
        jobs[address] = job
        job.start()
    }

    /** Waits [ms], or less if [retryStopped] is called meanwhile. */
    private suspend fun pause(ms: Long) {
        val seen = retrySignal.value
        withTimeoutOrNull(ms) { retrySignal.first { it != seen } }
    }

    /**
     * Gives everything that was stopped another go right away, instead of at the end of its pause:
     * devices whose ingest key the broker rejected ([DeviceError.Kind.AUTH_REJECTED]), devices the
     * [DeviceFilter] turned down, devices taken out with [detach], and buffered data of absent devices
     * whose upload gave up (e.g. on a rejected key). Call it when the credentials or what the filter
     * accepts have changed.
     */
    public fun retryStopped() {
        detached.clear()
        retrySignal.update { it + 1 }
        if (scope.isActive) scope.launch { recoverStoredBuffers() }
    }

    // ---- attach mode -----------------------------------------------------------------------------

    /**
     * Creates the handle for relaying a device over a GATT the host app owns. Nothing happens until the
     * host has wired [GattAttachment.gattCallback] and calls [GattAttachment.start].
     *
     * @param pollRssi read RSSI periodically for [GatewayDeviceState.rssi] (while [devices] is being
     *   collected). Off by default, since it issues extra operations on the host's GATT.
     */
    public fun newAttachment(pollRssi: Boolean = false): GattAttachment =
        GattAttachment(this, requestedMtu, pollRssi) { gatt -> platform.isGattConnected(gatt.device) }

    /** Runs a single-shot session on a connection the host owns (it owns reconnection too). */
    internal fun startAttached(connection: BleConnection, pollRssi: Boolean) {
        val address = connection.deviceAddress
        detached -= address
        tracked += address
        val previous = jobs.remove(address)
        jobs[address] = scope.launch {
            // Let a previous session for this address release its cloud link first, or the new one
            // would be refused as a duplicate of the same device ID.
            previous?.cancelAndJoin()
            try {
                GatewaySession(
                    connection, mqttConfig, links, statusOf(address), deviceFilter, rssiWanted.takeIf { pollRssi },
                    artifacts,
                ).run()
            } catch (c: CancellationException) {
                throw c // normal teardown — not an error
            } catch (t: DeviceRejectedException) {
                Slog.w(TAG, "ignoring $address: ${t.message}")
                untrack(address)
            } catch (t: Throwable) {
                Slog.w(TAG, "attached session for $address failed: ${t.message}")
                statusOf(address).invoke { it.copy(error = errorOf(t)) }
            } finally {
                // An attached session is single-shot: once it is over, the address is free again — for
                // the next start(), or for the scan to pick the device up in managed mode.
                jobs.remove(address, coroutineContext.job)
            }
        }
    }

    /**
     * Stops gatewaying a device (managed or attached) and cancels its session. Data it already buffered
     * is still uploaded in the background. The scan leaves the device alone from then on, until
     * [retryStopped] is called or it is attached again.
     */
    public fun detach(address: String) {
        detached += address
        untrack(address)
        jobs.remove(address)?.cancel()
    }

    /**
     * Stops everything and releases resources. Unsent data is written to the flash tier (where there is
     * one) in the background; a gateway created afterwards — or already running — picks it up.
     */
    public fun shutdown() {
        wantScanning = false
        platform.stop()
        jobs.values.forEach { it.cancel() }
        jobs.clear()
        scope.cancel()
        artifacts?.clear()
        // The flash store closes once every link has flushed and closed its buffer.
        buffers.shutdown()
    }

    /**
     * The status updater for [address]: every update — from any BLE session or the device's cloud link —
     * is applied atomically to the one entry in [devices], so concurrent writers never overwrite each
     * other with stale copies. The tracked check runs inside update(): if a detach wins the race, the CAS
     * retries and sees it untracked, so a late update can't resurrect a detached device.
     */
    private fun statusOf(address: String): StatusPush = { change ->
        _devices.update { map ->
            if (address in tracked) map + (address to change(map[address] ?: GatewayDeviceState(address))) else map
        }
    }

    private fun untrack(address: String) {
        tracked -= address
        _devices.update { it - address }
    }

    private fun errorOf(error: Throwable): DeviceError {
        val kind = when (error) {
            is MqttAuthException -> DeviceError.Kind.AUTH_REJECTED
            is DuplicateDeviceException -> DeviceError.Kind.DUPLICATE_DEVICE
            else -> DeviceError.Kind.BLUETOOTH
        }
        return DeviceError(kind, error.message ?: error.javaClass.simpleName)
    }

    public companion object {
        /** The largest ATT MTU Android negotiates; what the gateway asks for unless told otherwise. */
        public const val DEFAULT_MTU: Int = 517

        private const val TAG = "SpotflowGateway"
        private const val SCAN_JOB_KEY = "__scan__"
        private const val INITIAL_BACKOFF_MS = 2_000L
        private const val RECONNECT_DELAY_MS = 500L
        private const val MAX_BACKOFF_MS = 30_000L

        /** How long the scan runs at the quicker `BALANCED` duty cycle after starting. */
        private const val SCAN_BOOST_MS = 2 * 60_000L

        /** A running scan is restarted this often, inside Android's 30-minute limit. */
        private const val SCAN_RESTART_MS = 25 * 60_000L

        // Android allows 5 scan starts per 30 s; this backoff stays well inside that.
        private const val SCAN_RETRY_INITIAL_MS = 5_000L
        private const val SCAN_RETRY_MAX_MS = 60_000L

        private const val AUTH_RETRY_INITIAL_MS = 60_000L
        private const val AUTH_RETRY_MAX_MS = 15 * 60_000L

        /** How long a device the filter rejected is left alone before it is asked about again. */
        private const val REJECTED_RETRY_MS = 10 * 60_000L
    }
}

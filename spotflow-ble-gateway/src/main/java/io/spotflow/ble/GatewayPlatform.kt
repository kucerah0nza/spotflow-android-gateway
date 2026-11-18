package io.spotflow.ble

import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.ConnectivityManager
import android.net.Network
import androidx.core.content.ContextCompat
import io.spotflow.ble.cloud.CredentialsProvider
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.MqttUplink
import io.spotflow.ble.cloud.Uplink
import io.spotflow.ble.ota.ArtifactDownloader
import io.spotflow.ble.ota.HttpArtifactDownloader
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ManagedBleConnection
import io.spotflow.ble.transport.SpotflowScanner
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

/** A device found by scanning, and how to connect to it. */
internal class DiscoveredDevice(
    val address: String,
    /** Opens a connection; `autoConnect` lets Android re-attach whenever the device reappears. */
    val connect: (autoConnect: Boolean) -> BleConnection,
)

/**
 * Everything [SpotflowGateway] needs from Android and from the network, behind one seam: the Bluetooth
 * adapter and scanner, GATT connections, connectivity, and the MQTT client. The gateway's own logic —
 * who is connected, when to retry, what to report — sits on top of it and is tested against a fake.
 */
internal interface GatewayPlatform {
    val isBluetoothEnabled: Boolean

    /** Whether the phone has a network. Stays `true` if that cannot be observed. */
    val online: StateFlow<Boolean>

    /** Starts watching the Bluetooth adapter and the network. */
    fun start(onBluetoothOff: () -> Unit, onBluetoothOn: () -> Unit)

    fun stop()

    /**
     * Devices advertising the Spotflow service, for as long as the flow is collected. Fails if the scan
     * cannot be started or stops working.
     */
    fun scan(lowPower: Boolean): Flow<DiscoveredDevice>

    fun createUplink(deviceId: String): Uplink

    /** Downloads OTA artifacts. */
    val artifactDownloader: ArtifactDownloader get() = HttpArtifactDownloader()

    /** Whether [device] has a GATT link up right now (attach mode). */
    fun isGattConnected(device: BluetoothDevice): Boolean
}

internal class AndroidGatewayPlatform(
    private val appContext: Context,
    private val credentials: CredentialsProvider,
    private val mqttConfig: MqttConfig,
    private val requestedMtu: Int,
) : GatewayPlatform {

    private val bluetoothManager: BluetoothManager?
        get() = appContext.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager

    private val network = NetworkMonitor(appContext)

    override val isBluetoothEnabled: Boolean get() = bluetoothManager?.adapter?.isEnabled == true
    override val online: StateFlow<Boolean> get() = network.online

    private var bluetoothReceiver: BroadcastReceiver? = null

    /**
     * Watches the Bluetooth adapter itself. Turning Bluetooth off often does not deliver a GATT
     * disconnect callback, which would otherwise leave sessions parked forever; observing the adapter
     * lets the gateway tear those sessions down and rediscover once Bluetooth returns — even with the
     * screen off / app backgrounded, since the gateway runs in a foreground service.
     */
    @Synchronized
    override fun start(onBluetoothOff: () -> Unit, onBluetoothOn: () -> Unit) {
        if (bluetoothReceiver != null) return
        val receiver = object : BroadcastReceiver() {
            override fun onReceive(context: Context, intent: Intent) {
                if (intent.action != BluetoothAdapter.ACTION_STATE_CHANGED) return
                when (intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)) {
                    BluetoothAdapter.STATE_TURNING_OFF, BluetoothAdapter.STATE_OFF -> onBluetoothOff()
                    BluetoothAdapter.STATE_ON -> onBluetoothOn()
                }
            }
        }
        ContextCompat.registerReceiver(
            appContext,
            receiver,
            IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED),
            ContextCompat.RECEIVER_NOT_EXPORTED,
        )
        bluetoothReceiver = receiver
        network.start()
    }

    @Synchronized
    override fun stop() {
        bluetoothReceiver?.let { runCatching { appContext.unregisterReceiver(it) } }
        bluetoothReceiver = null
        network.stop()
    }

    override fun scan(lowPower: Boolean): Flow<DiscoveredDevice> {
        val adapter = bluetoothManager?.adapter
            ?: return flow { throw IllegalStateException("this device has no Bluetooth") }
        return SpotflowScanner(adapter).scan(lowPower).map { device ->
            DiscoveredDevice(device.address) { autoConnect ->
                ManagedBleConnection(appContext, device, requestedMtu, autoConnect)
            }
        }
    }

    override fun createUplink(deviceId: String): Uplink = MqttUplink(deviceId, credentials, mqttConfig)

    // The host is responsible for holding BLUETOOTH_CONNECT (documented in the README/manifest).
    @SuppressLint("MissingPermission")
    override fun isGattConnected(device: BluetoothDevice): Boolean = runCatching {
        bluetoothManager?.getConnectionState(device, BluetoothProfile.GATT) == BluetoothProfile.STATE_CONNECTED
    }.getOrDefault(false)
}

/**
 * Tells whether the phone has a network, so uploads can wait for one instead of retrying into the void,
 * and resume the moment it is back rather than at the end of a backoff.
 */
internal class NetworkMonitor(context: Context) {
    private val manager = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager

    private val _online = MutableStateFlow(true)

    /** Whether there is a default network. `true` if it cannot be observed, so nothing ever waits in vain. */
    val online: StateFlow<Boolean> = _online

    private val callback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) {
            _online.value = true
        }

        override fun onLost(network: Network) {
            // Sent for the default network when nothing replaces it (a switch between networks reports
            // the new one as available instead).
            _online.value = false
        }
    }

    private var registered = false

    @Synchronized
    fun start() {
        if (registered || manager == null) return
        try {
            // The callback only reports changes from here on, and says nothing if there is no network.
            _online.value = manager.activeNetwork != null
            manager.registerDefaultNetworkCallback(callback)
            registered = true
        } catch (e: RuntimeException) {
            // e.g. the host removed ACCESS_NETWORK_STATE from its manifest. Assume a network and let the
            // connection attempts find out.
            Slog.w(TAG, "cannot watch connectivity, assuming the phone is online: ${e.message}")
            _online.value = true
        }
    }

    @Synchronized
    fun stop() {
        if (!registered) return
        runCatching { manager?.unregisterNetworkCallback(callback) }
        registered = false
        _online.value = true
    }

    private companion object {
        const val TAG = "SpotflowGateway"
    }
}

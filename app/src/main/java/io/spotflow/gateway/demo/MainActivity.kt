package io.spotflow.gateway.demo

import android.Manifest
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.os.Bundle
import android.view.View
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.updatePadding
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import io.spotflow.ble.GatewayDeviceState
import io.spotflow.ble.SpotflowGateway
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.StaticIngestKey
import io.spotflow.ble.service.SpotflowGatewayService
import io.spotflow.ble.transport.ConnectionState
import io.spotflow.gateway.demo.databinding.ActivityMainBinding
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * Minimal reference gateway: enter a Spotflow ingest key, grant BLE + notification permissions, and the
 * app starts a foreground [SpotflowGatewayService] that scans for Spotflow devices and relays their
 * diagnostics to the cloud — continuing while the screen is off.
 */
class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val keyStore by lazy { IngestKeyStore(this) }

    private val bluetoothAdapter: BluetoothAdapter? by lazy {
        (getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter
    }

    private val permissionLauncher =
        registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { grants ->
            if (grants.values.all { it }) {
                ensureBluetoothThenStart()
            } else {
                Toast.makeText(this, R.string.permissions_required, Toast.LENGTH_LONG).show()
            }
        }

    private val enableBluetoothLauncher =
        registerForActivityResult(ActivityResultContracts.StartActivityForResult()) {
            if (bluetoothAdapter?.isEnabled == true) {
                // Initial-start flow. A mid-run re-enable is handled by bluetoothStateReceiver.
                if (!isGatewayRunning) startGateway()
            } else {
                Toast.makeText(this, R.string.bluetooth_required, Toast.LENGTH_LONG).show()
            }
        }

    private val isGatewayRunning: Boolean get() = SpotflowGatewayService.gateway != null

    /** Watches for Bluetooth being toggled while the gateway is running. */
    private val bluetoothStateReceiver = object : BroadcastReceiver() {
        @android.annotation.SuppressLint("MissingPermission")
        override fun onReceive(context: Context, intent: Intent) {
            if (intent.action != BluetoothAdapter.ACTION_STATE_CHANGED) return
            when (intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)) {
                BluetoothAdapter.STATE_OFF ->
                    if (isGatewayRunning) binding.btBanner.visibility = View.VISIBLE
                BluetoothAdapter.STATE_ON -> {
                    binding.btBanner.visibility = View.GONE
                    if (isGatewayRunning) SpotflowGatewayService.gateway?.startScanning()
                }
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // Android 15 draws edge-to-edge by default; pad the root for the system bars and the keyboard.
        ViewCompat.setOnApplyWindowInsetsListener(binding.root) { view, insets ->
            val bars = insets.getInsets(
                WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.ime(),
            )
            view.updatePadding(top = bars.top, bottom = bars.bottom)
            insets
        }

        // Prefill the previously saved ingest key and buffer sizes.
        binding.ingestKey.setText(keyStore.ingestKey)
        binding.bufferRamMb.setText(keyStore.bufferRamMb.toString())
        binding.bufferFlashMb.setText(keyStore.bufferFlashMb.toString())

        binding.startButton.setOnClickListener {
            val key = binding.ingestKey.text?.toString()?.trim().orEmpty()
            if (key.isEmpty()) {
                Toast.makeText(this, R.string.enter_key_first, Toast.LENGTH_SHORT).show()
            } else {
                requestPermissionsThenStart()
            }
        }

        binding.stopButton.setOnClickListener { stopGateway() }

        binding.btBanner.setOnClickListener {
            enableBluetoothLauncher.launch(Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE))
        }

        observeDevices()
    }

    override fun onStart() {
        super.onStart()
        ContextCompat.registerReceiver(
            this,
            bluetoothStateReceiver,
            IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED),
            ContextCompat.RECEIVER_NOT_EXPORTED,
        )
        // Reflect the current state (e.g. BT turned off while the app was backgrounded).
        binding.btBanner.visibility =
            if (isGatewayRunning && bluetoothAdapter?.isEnabled != true) View.VISIBLE else View.GONE
    }

    override fun onStop() {
        super.onStop()
        runCatching { unregisterReceiver(bluetoothStateReceiver) }
    }

    private fun requestPermissionsThenStart() {
        val needed = buildList {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                add(Manifest.permission.BLUETOOTH_SCAN)
                add(Manifest.permission.BLUETOOTH_CONNECT)
            } else {
                add(Manifest.permission.ACCESS_FINE_LOCATION)
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                add(Manifest.permission.POST_NOTIFICATIONS)
            }
        }
        permissionLauncher.launch(needed.toTypedArray())
    }

    /** Ensures Bluetooth is on before starting; prompts the user to enable it if needed. */
    private fun ensureBluetoothThenStart() {
        val adapter = bluetoothAdapter
        when {
            adapter == null ->
                Toast.makeText(this, R.string.no_bluetooth, Toast.LENGTH_LONG).show()
            !adapter.isEnabled ->
                enableBluetoothLauncher.launch(Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE))
            else -> startGateway()
        }
    }

    private fun startGateway() {
        val key = binding.ingestKey.text?.toString()?.trim().orEmpty()
        // 0 is allowed for either tier: RAM 0 = spill to flash immediately (minimal crash-loss risk);
        // flash 0 = RAM-only, no persistence.
        val ramMb = binding.bufferRamMb.text?.toString()?.toIntOrNull()?.coerceAtLeast(0)
            ?: keyStore.bufferRamMb
        val flashMb = binding.bufferFlashMb.text?.toString()?.toIntOrNull()?.coerceAtLeast(0)
            ?: keyStore.bufferFlashMb
        keyStore.ingestKey = key // persist across restarts
        keyStore.bufferRamMb = ramMb
        keyStore.bufferFlashMb = flashMb

        val config = MqttConfig(
            bufferMaxBytes = (ramMb + flashMb).toLong() * 1024L * 1024L,
            ramBufferMaxBytes = ramMb.toLong() * 1024L * 1024L,
        )
        SpotflowGatewayService.gatewayFactory = { ctx -> SpotflowGateway(ctx, StaticIngestKey(key), config) }
        SpotflowGatewayService.onReady = { gateway -> gateway.startScanning() }
        SpotflowGatewayService.start(this)

        binding.startButton.isEnabled = false
        binding.stopButton.isEnabled = true
        binding.ingestKeyLayout.isEnabled = false
        binding.bufferRamMb.isEnabled = false
        binding.bufferFlashMb.isEnabled = false
        binding.errorBanner.visibility = View.GONE
        binding.status.text = getString(R.string.scanning)
    }

    private fun stopGateway() {
        SpotflowGatewayService.stop(this)
        binding.startButton.isEnabled = true
        binding.stopButton.isEnabled = false
        binding.ingestKeyLayout.isEnabled = true
        binding.bufferRamMb.isEnabled = true
        binding.bufferFlashMb.isEnabled = true
        binding.errorBanner.visibility = View.GONE
        binding.btBanner.visibility = View.GONE
        binding.status.text = getString(R.string.idle)
    }

    private fun observeDevices() {
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                // The service creates a fresh gateway on each Start, so track the current one and
                // re-subscribe when it changes (otherwise the UI keeps mirroring a shut-down gateway).
                var current: SpotflowGateway? = null
                var collectJob: Job? = null
                while (true) {
                    val gateway = SpotflowGatewayService.gateway
                    if (gateway !== current) {
                        current = gateway
                        collectJob?.cancel()
                        collectJob = gateway?.let { g ->
                            launch { g.devices.collect { render(it.values.toList()) } }
                        }
                    }
                    delay(300)
                }
            }
        }
    }

    private fun render(devices: List<GatewayDeviceState>) {
        if (devices.isEmpty()) {
            binding.status.text = getString(R.string.scanning)
        } else {
            binding.status.text = devices.joinToString("\n\n") { formatDevice(it) }
        }

        val error = devices.firstNotNullOfOrNull { it.error }
        if (error == null) {
            binding.errorBanner.visibility = View.GONE
        } else {
            val hint = if (error.contains("ingest key", ignoreCase = true)) {
                "\n${getString(R.string.auth_hint)}"
            } else {
                ""
            }
            binding.errorBanner.text = "⚠  $error$hint"
            binding.errorBanner.visibility = View.VISIBLE
        }
    }

    private fun formatDevice(d: GatewayDeviceState): String {
        val cloud = if (d.cloudConnected) "connected" else "offline"
        val marker = when {
            d.error != null -> "✗"
            d.cloudConnected && d.ble == ConnectionState.READY -> "●"
            else -> "…"
        }
        fun line(label: String, value: String) = "    ${"$label:".padEnd(17)}$value"
        // Only show a live signal reading while actually connected — otherwise it would keep
        // displaying the last RSSI next to "DISCONNECTED", which reads as contradictory.
        val signal = d.rssi?.takeIf { d.ble == ConnectionState.READY }
            ?.let { "$it dBm · ${signalQuality(it)}" } ?: "—"
        return buildString {
            appendLine("$marker ${d.deviceId ?: d.address}")
            appendLine(line("BLE device", d.ble.toString()))
            appendLine(line("Signal", signal))
            appendLine(line("MQTT connection", cloud))
            appendLine(line("Forwarded", "${d.forwarded} msgs"))
            appendLine(line("Buffer RAM", humanBytes(d.ramBytes)))
            append(line("Buffer flash", humanBytes(d.diskBytes)))
        }
    }

    private fun signalQuality(rssi: Int): String = when {
        rssi >= -60 -> "strong"
        rssi >= -75 -> "good"
        rssi >= -85 -> "weak"
        else -> "very weak"
    }

    private fun humanBytes(bytes: Long): String = when {
        bytes >= 1024 * 1024 -> "%.1f MB".format(bytes / (1024.0 * 1024.0))
        bytes >= 1024 -> "%.1f KB".format(bytes / 1024.0)
        else -> "$bytes B"
    }
}

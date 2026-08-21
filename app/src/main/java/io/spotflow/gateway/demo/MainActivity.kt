package io.spotflow.gateway.demo

import android.Manifest
import android.os.Build
import android.os.Bundle
import android.view.View
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.updatePadding
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import io.spotflow.ble.GatewayDeviceState
import io.spotflow.ble.SpotflowGateway
import io.spotflow.ble.cloud.StaticIngestKey
import io.spotflow.ble.service.SpotflowGatewayService
import io.spotflow.ble.transport.ConnectionState
import io.spotflow.gateway.demo.databinding.ActivityMainBinding
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * Minimal reference gateway: enter a Spotflow ingest key, grant BLE + notification permissions, and the
 * app starts a foreground [SpotflowGatewayService] that scans for Spotflow devices and relays their
 * diagnostics to the cloud — continuing while the screen is off.
 */
class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding

    private val permissionLauncher =
        registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { grants ->
            if (grants.values.all { it }) {
                startGateway()
            } else {
                Toast.makeText(this, R.string.permissions_required, Toast.LENGTH_LONG).show()
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

        binding.startButton.setOnClickListener {
            val key = binding.ingestKey.text?.toString()?.trim().orEmpty()
            if (key.isEmpty()) {
                Toast.makeText(this, R.string.enter_key_first, Toast.LENGTH_SHORT).show()
            } else {
                requestPermissionsThenStart()
            }
        }

        binding.stopButton.setOnClickListener { stopGateway() }

        observeDevices()
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

    private fun startGateway() {
        val key = binding.ingestKey.text?.toString()?.trim().orEmpty()
        SpotflowGatewayService.gatewayFactory = { ctx -> SpotflowGateway(ctx, StaticIngestKey(key)) }
        SpotflowGatewayService.onReady = { gateway -> gateway.startScanning() }
        SpotflowGatewayService.start(this)

        binding.startButton.isEnabled = false
        binding.stopButton.isEnabled = true
        binding.ingestKeyLayout.isEnabled = false
        binding.errorBanner.visibility = View.GONE
        binding.status.text = getString(R.string.scanning)
    }

    private fun stopGateway() {
        SpotflowGatewayService.stop(this)
        binding.startButton.isEnabled = true
        binding.stopButton.isEnabled = false
        binding.ingestKeyLayout.isEnabled = true
        binding.errorBanner.visibility = View.GONE
        binding.status.text = getString(R.string.idle)
    }

    private fun observeDevices() {
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                // Wait for the service to create the gateway, then mirror its device flow.
                var gateway: SpotflowGateway? = null
                while (gateway == null) {
                    gateway = SpotflowGatewayService.gateway
                    if (gateway == null) delay(300)
                }
                gateway.devices.collect { devices -> render(devices.values.toList()) }
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
        return buildString {
            appendLine("$marker ${d.deviceId ?: d.address}")
            appendLine("    ble:   ${d.ble}")
            appendLine("    cloud: $cloud")
            append("    fwd:   ${d.forwarded} msgs")
        }
    }
}

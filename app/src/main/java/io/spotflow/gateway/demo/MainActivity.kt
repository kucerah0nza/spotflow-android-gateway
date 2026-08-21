package io.spotflow.gateway.demo

import android.Manifest
import android.os.Build
import android.os.Bundle
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.updatePadding
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import io.spotflow.ble.SpotflowGateway
import io.spotflow.ble.cloud.StaticIngestKey
import io.spotflow.ble.service.SpotflowGatewayService
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
                Toast.makeText(this, "Permissions are required to run the gateway", Toast.LENGTH_LONG).show()
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
                Toast.makeText(this, "Enter an ingest key first", Toast.LENGTH_SHORT).show()
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
        binding.status.text = "Scanning for Spotflow devices…"
    }

    private fun stopGateway() {
        SpotflowGatewayService.stop(this)
        binding.startButton.isEnabled = true
        binding.stopButton.isEnabled = false
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
                gateway.devices.collect { devices ->
                    if (devices.isEmpty()) return@collect
                    binding.status.text = devices.values.joinToString("\n\n") { d ->
                        buildString {
                            appendLine("device: ${d.deviceId ?: d.address}")
                            appendLine("  ble:    ${d.ble}")
                            appendLine("  cloud:  ${if (d.cloudConnected) "connected" else "offline"}")
                            appendLine("  fwd:    ${d.forwarded} msgs")
                            d.error?.let { appendLine("  error:  $it") }
                        }
                    }
                }
            }
        }
    }
}

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
import android.text.SpannableString
import android.text.Spanned
import android.text.style.ForegroundColorSpan
import android.view.View
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.view.ViewCompat
import androidx.core.view.isVisible
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.updatePadding
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import io.spotflow.ble.GatewayDeviceState
import io.spotflow.ble.SpotflowGateway
import io.spotflow.ble.cloud.BufferUsage
import io.spotflow.ble.service.SpotflowGatewayService
import io.spotflow.ble.transport.ConnectionState
import io.spotflow.gateway.demo.databinding.ActivityMainBinding
import io.spotflow.gateway.demo.databinding.ItemDeviceBinding
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * Minimal reference gateway: set a Spotflow ingest key in [SettingsActivity], grant BLE + notification
 * permissions, and the app starts a foreground [SpotflowGatewayService] that scans for Spotflow devices and relays their
 * diagnostics to the cloud — continuing while the screen is off.
 */
class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val keyStore by lazy { IngestKeyStore(this) }

    /** One view per device shown, by Bluetooth address — updated in place on every state change. */
    private val deviceViews = LinkedHashMap<String, ItemDeviceBinding>()

    /** Devices whose details the user collapsed (by address); the rest are shown expanded. */
    private val collapsed = mutableSetOf<String>()

    private val bluetoothAdapter: BluetoothAdapter? by lazy {
        (getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)?.adapter
    }

    private val permissionLauncher =
        registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { grants ->
            // Notifications are optional: the foreground service runs without them (the user just
            // doesn't see its notification). Only the Bluetooth/location permissions are required.
            if (grants.filterKeys { it != Manifest.permission.POST_NOTIFICATIONS }.values.all { it }) {
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

        binding.subtitle.text = getString(R.string.subtitle, BuildConfig.VERSION_NAME)
        savedInstanceState?.getStringArrayList(STATE_COLLAPSED)?.let { collapsed += it }

        binding.settingsButton.setOnClickListener { openSettings() }

        binding.startButton.setOnClickListener {
            if (keyStore.ingestKey.isNullOrBlank()) {
                Toast.makeText(this, R.string.enter_key_first, Toast.LENGTH_SHORT).show()
                openSettings()
            } else {
                requestPermissionsThenStart()
            }
        }

        binding.stopButton.setOnClickListener { stopGateway() }
        binding.restartButton.setOnClickListener { restartGateway() }

        binding.btBanner.setOnClickListener {
            enableBluetoothLauncher.launch(Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE))
        }

        observeDevices()
    }

    override fun onResume() {
        super.onResume()
        // The gateway may be running without this activity having started it (activity recreated after
        // rotation, app reopened, or the service restored after a process restart) — or it may have been
        // left enabled but not be running (app updated, phone rebooted): then resume it. This also covers
        // the window right after Start, before the service has created its gateway.
        val running = isGatewayRunning || resumeGatewayIfEnabled(this, keyStore)
        syncControls(running = running)
        if (!running) showIdle()
        updateSettingsBanner()
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        outState.putStringArrayList(STATE_COLLAPSED, ArrayList(collapsed))
    }

    private fun openSettings() = startActivity(Intent(this, SettingsActivity::class.java))

    private fun idleText(): String =
        getString(if (keyStore.ingestKey.isNullOrBlank()) R.string.idle_no_key else R.string.idle)

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
        if (!configureGatewayService(keyStore)) { // e.g. the stored key could not be decrypted
            Toast.makeText(this, R.string.enter_key_first, Toast.LENGTH_SHORT).show()
            return
        }
        keyStore.gatewayEnabled = true // GatewayApp restores the service hooks after a process restart
        SpotflowGatewayService.start(this)

        syncControls(running = true)
        binding.errorBanner.visibility = View.GONE
        render(emptyList())
    }

    /**
     * Applies saved settings by restarting the gateway in place: the service stays in the foreground and
     * devices reconnect within seconds, so relaying pauses far shorter than with Stop and Start.
     */
    private fun restartGateway() {
        if (!configureGatewayService(keyStore)) {
            Toast.makeText(this, R.string.enter_key_first, Toast.LENGTH_SHORT).show()
            return
        }
        SpotflowGatewayService.restart(this)
        binding.errorBanner.visibility = View.GONE
        binding.settingsBanner.isVisible = false
    }

    private fun stopGateway() {
        keyStore.gatewayEnabled = false
        SpotflowGatewayService.stop(this)
        syncControls(running = false)
        binding.errorBanner.visibility = View.GONE
        binding.btBanner.visibility = View.GONE
        binding.settingsBanner.isVisible = false
        showIdle()
    }

    /** Shows the restart prompt while the running gateway uses older settings than those saved. */
    private fun updateSettingsBanner() {
        binding.settingsBanner.isVisible = isGatewayRunning && keyStore.hasUnappliedSettings
    }

    private fun showIdle() {
        render(emptyList())
        binding.status.text = idleText()
        binding.bufferSummary.isVisible = false
    }

    /** The gateway-wide buffer line; amber once either tier is over 80% full (eviction is near). */
    private fun renderUsage(usage: BufferUsage) {
        if (!isGatewayRunning) return
        val ram = humanBytes(usage.ramBytes)
        val ramMax = limitText(usage.ramMaxBytes)
        binding.bufferSummary.text = if (usage.flashMaxBytes > 0) {
            getString(R.string.buffer_summary, ram, ramMax, humanBytes(usage.flashBytes), limitText(usage.flashMaxBytes))
        } else {
            getString(R.string.buffer_summary_ram_only, ram, ramMax)
        }
        val nearlyFull = usage.ramBytes > usage.ramMaxBytes * 0.8 ||
            (usage.flashMaxBytes > 0 && usage.flashBytes > usage.flashMaxBytes * 0.8)
        binding.bufferSummary.setTextColor(
            if (nearlyFull) {
                ContextCompat.getColor(this, R.color.status_pending)
            } else {
                binding.subtitle.currentTextColor
            },
        )
        binding.bufferSummary.alpha = if (nearlyFull) 1f else 0.7f
        binding.bufferSummary.isVisible = true
    }

    /** Enables Start/Stop to match whether the gateway is running. */
    private fun syncControls(running: Boolean) {
        binding.startButton.isEnabled = !running
        binding.stopButton.isEnabled = running
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
                        updateSettingsBanner() // a new gateway was built from the saved settings
                        collectJob?.cancel()
                        collectJob = gateway?.let { g ->
                            launch {
                                launch { g.devices.collect { render(it.values.toList()) } }
                                launch { g.bufferUsage.collect { renderUsage(it) } }
                            }
                        }
                    }
                    delay(300)
                }
            }
        }
    }

    private fun render(devices: List<GatewayDeviceState>) {
        binding.status.isVisible = devices.isEmpty()
        binding.status.text = getString(R.string.scanning)

        // Update the device views in place (so a collapsed device stays collapsed), in the gateway's order.
        val shown = devices.map { it.address }.toSet()
        deviceViews.keys.filter { it !in shown }.forEach { address ->
            binding.deviceList.removeView(deviceViews.remove(address)!!.root)
        }
        devices.forEachIndexed { index, device ->
            val item = deviceViews.getOrPut(device.address) { createDeviceView(device.address) }
            if (binding.deviceList.getChildAt(index) !== item.root) {
                binding.deviceList.removeView(item.root)
                binding.deviceList.addView(item.root, index)
            }
            item.name.text = deviceTitle(device)
            item.details.text = formatDetails(device)
            showExpanded(item, device.address !in collapsed, animate = false)
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

    private fun createDeviceView(address: String): ItemDeviceBinding =
        ItemDeviceBinding.inflate(layoutInflater, binding.deviceList, false).also { item ->
            item.header.setOnClickListener {
                val expand = address in collapsed
                if (expand) collapsed -= address else collapsed += address
                showExpanded(item, expand, animate = true)
            }
        }

    /** Shows or hides a device's details; the chevron points down when expanded, sideways when not. */
    private fun showExpanded(item: ItemDeviceBinding, expanded: Boolean, animate: Boolean) {
        item.details.isVisible = expanded
        val rotation = if (expanded) 0f else if (item.root.layoutDirection == View.LAYOUT_DIRECTION_RTL) 90f else -90f
        if (animate) item.chevron.animate().rotation(rotation).setDuration(150).start() else item.chevron.rotation = rotation
        ViewCompat.setStateDescription(
            item.header,
            getString(if (expanded) R.string.expanded else R.string.collapsed),
        )
    }

    /**
     * The device name led by a status marker: green ● when relaying (BLE ready and MQTT connected),
     * amber … while connecting or offline, red ✗ on an error. Glyphs differ too, not just colors.
     */
    private fun deviceTitle(d: GatewayDeviceState): CharSequence {
        val (marker, color) = when {
            d.error != null -> "✗" to R.color.status_error
            d.cloudConnected && d.ble == ConnectionState.READY -> "●" to R.color.status_ok
            else -> "…" to R.color.status_pending
        }
        return SpannableString("$marker ${d.deviceId ?: d.address}").apply {
            setSpan(
                ForegroundColorSpan(ContextCompat.getColor(this@MainActivity, color)),
                0,
                marker.length,
                Spanned.SPAN_EXCLUSIVE_EXCLUSIVE,
            )
        }
    }

    private fun formatDetails(d: GatewayDeviceState): String {
        val cloud = if (d.cloudConnected) "connected" else "offline"
        fun line(label: String, value: String) = "    ${"$label:".padEnd(17)}$value"
        // Only show a live signal reading while actually connected — otherwise it would keep
        // displaying the last RSSI next to "DISCONNECTED", which reads as contradictory.
        val signal = d.rssi?.takeIf { d.ble == ConnectionState.READY }
            ?.let { "$it dBm · ${signalQuality(it)}" } ?: "—"
        return buildString {
            appendLine(line("BLE device", d.ble.toString()))
            appendLine(line("Signal", signal))
            appendLine(line("MQTT connection", cloud))
            appendLine(line("Forwarded", "${d.forwarded} msgs"))
            appendLine(line("Received", "${d.received} msgs"))
            // Messages not yet uploaded; where they sit (RAM or flash) is shown for the whole gateway above.
            append(line("Waiting", "${d.pendingMessages} msgs · ${humanBytes(d.ramBytes + d.diskBytes)}"))
        }
    }

    private fun signalQuality(rssi: Int): String = when {
        rssi >= -60 -> "strong"
        rssi >= -75 -> "good"
        rssi >= -85 -> "weak"
        else -> "very weak"
    }

    /** A configured limit: whole megabytes as set in Settings (e.g. "50 MB"). */
    private fun limitText(bytes: Long): String =
        if (bytes % (1024 * 1024) == 0L) "${bytes / (1024 * 1024)} MB" else humanBytes(bytes)

    private fun humanBytes(bytes: Long): String = when {
        bytes >= 1024 * 1024 -> "%.1f MB".format(bytes / (1024.0 * 1024.0))
        bytes >= 1024 -> "%.1f KB".format(bytes / 1024.0)
        else -> "$bytes B"
    }

    private companion object {
        const val STATE_COLLAPSED = "collapsed_devices"
    }
}

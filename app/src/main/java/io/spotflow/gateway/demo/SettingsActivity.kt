package io.spotflow.gateway.demo

import android.os.Bundle
import android.view.View
import android.widget.Toast
import androidx.activity.OnBackPressedCallback
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.updatePadding
import androidx.core.widget.doAfterTextChanged
import com.google.android.material.dialog.MaterialAlertDialogBuilder
import com.google.android.material.textfield.TextInputLayout
import io.spotflow.ble.service.SpotflowGatewayService
import io.spotflow.gateway.demo.databinding.ActivitySettingsBinding

/**
 * The demo's settings: the ingest key and the store-and-forward buffer sizes. Changes are kept only when
 * the user taps Save; Cancel or Back discards them (asking first if anything was edited). Settings can be
 * changed while the gateway runs — it keeps relaying with the old ones until it is restarted from the main
 * screen, which takes seconds rather than the minutes a Stop/edit/Start round could leave devices waiting.
 */
class SettingsActivity : AppCompatActivity() {

    private lateinit var binding: ActivitySettingsBinding
    private val store by lazy { IngestKeyStore(this) }

    private val gatewayRunning: Boolean get() = SpotflowGatewayService.gateway != null || store.gatewayEnabled

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivitySettingsBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // Android 15 draws edge-to-edge by default; pad the root for the system bars and the keyboard.
        ViewCompat.setOnApplyWindowInsetsListener(binding.root) { view, insets ->
            val bars = insets.getInsets(
                WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.ime(),
            )
            view.updatePadding(top = bars.top, bottom = bars.bottom)
            insets
        }

        // On rotation the fields restore what the user typed; only prefill on first open.
        if (savedInstanceState == null) {
            binding.ingestKey.setText(store.ingestKey)
            binding.bufferRamMb.setText(store.bufferRamMb.toString())
            binding.bufferFlashMb.setText(store.bufferFlashMb.toString())
        }
        binding.ingestKey.doAfterTextChanged { binding.ingestKeyLayout.error = null }
        binding.bufferRamMb.doAfterTextChanged { binding.bufferRamLayout.error = null }
        binding.bufferFlashMb.doAfterTextChanged { binding.bufferFlashLayout.error = null }

        binding.toolbar.setNavigationOnClickListener { onBackPressedDispatcher.onBackPressed() }
        binding.cancelButton.setOnClickListener { finish() }
        binding.saveButton.setOnClickListener { save() }

        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                if (hasChanges()) confirmDiscard() else finish()
            }
        })
    }

    override fun onResume() {
        super.onResume()
        binding.runningNote.visibility = if (gatewayRunning) View.VISIBLE else View.GONE
    }

    private fun save() {
        // 0 is allowed for either tier: RAM 0 = spill to flash immediately (minimal crash-loss risk);
        // flash 0 = RAM-only, no persistence.
        val ramMb = megabytes(binding.bufferRamLayout, MAX_RAM_MB)
        val flashMb = megabytes(binding.bufferFlashLayout, MAX_FLASH_MB)
        val key = ingestKeyInput()
        binding.ingestKeyLayout.error = if (key.isEmpty()) getString(R.string.ingest_key_required) else null
        if (ramMb == null || flashMb == null || key.isEmpty()) return

        store.ingestKey = key
        store.bufferRamMb = ramMb
        store.bufferFlashMb = flashMb
        Toast.makeText(this, R.string.settings_saved, Toast.LENGTH_SHORT).show()
        finish()
    }

    /** Reads a buffer size field, or shows an error on it and returns null. */
    private fun megabytes(layout: TextInputLayout, max: Int): Int? {
        val value = layout.editText?.text?.toString()?.trim()?.toIntOrNull()?.takeIf { it in 0..max }
        layout.error = if (value == null) getString(R.string.buffer_invalid, max) else null
        return value
    }

    private fun ingestKeyInput(): String = binding.ingestKey.text?.toString()?.trim().orEmpty()

    private fun hasChanges(): Boolean =
        ingestKeyInput() != store.ingestKey.orEmpty() ||
            binding.bufferRamMb.text?.toString()?.trim() != store.bufferRamMb.toString() ||
            binding.bufferFlashMb.text?.toString()?.trim() != store.bufferFlashMb.toString()

    private fun confirmDiscard() {
        MaterialAlertDialogBuilder(this)
            .setTitle(R.string.discard_title)
            .setMessage(R.string.discard_message)
            .setPositiveButton(R.string.discard) { _, _ -> finish() }
            .setNegativeButton(R.string.keep_editing, null)
            .show()
    }

    private companion object {
        // Sanity caps, so a typo can't make the app run out of memory or fill the phone's storage.
        const val MAX_RAM_MB = 256
        const val MAX_FLASH_MB = 4096
    }
}

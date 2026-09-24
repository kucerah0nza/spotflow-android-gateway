package io.spotflow.gateway.demo

import android.content.Context
import android.content.SharedPreferences
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import android.util.Log
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * Persists the demo's settings across app restarts.
 *
 * The ingest key is a secret, so it is encrypted with an AES-GCM key held in the Android Keystore
 * (non-exportable) before it is written to preferences. If the Keystore is unavailable, or the stored
 * value cannot be decrypted (e.g. restored onto another device), the key is simply not persisted and the
 * user re-enters it — it is never stored in plain text.
 */
class IngestKeyStore(context: Context) {

    private val prefs: SharedPreferences = context.getSharedPreferences("spotflow_prefs", Context.MODE_PRIVATE)

    init {
        // Remove what earlier versions stored: a plain-text fallback copy of the key, and the
        // EncryptedSharedPreferences file (security-crypto is deprecated and no longer used).
        if (prefs.contains(LEGACY_KEY_INGEST)) prefs.edit().remove(LEGACY_KEY_INGEST).apply()
        context.deleteSharedPreferences(LEGACY_SECURE_PREFS)
    }

    var ingestKey: String?
        get() = prefs.getString(KEY_INGEST, null)?.let { decrypt(it) }
        set(value) {
            val encrypted = value?.takeIf { it.isNotBlank() }?.let { encrypt(it) }
            prefs.edit().apply {
                if (encrypted == null) remove(KEY_INGEST) else putString(KEY_INGEST, encrypted)
            }.apply()
        }

    /** RAM (in-memory) buffer tier size in megabytes (default 1; 0 = spill to flash immediately). */
    var bufferRamMb: Int
        get() = prefs.getInt(KEY_BUFFER_RAM_MB, DEFAULT_BUFFER_RAM_MB)
        set(value) = prefs.edit().putInt(KEY_BUFFER_RAM_MB, value.coerceAtLeast(0)).apply()

    /** Flash (persistent) buffer tier size in megabytes (default 50; 0 = RAM-only, never writes flash). */
    var bufferFlashMb: Int
        get() = prefs.getInt(KEY_BUFFER_FLASH_MB, DEFAULT_BUFFER_FLASH_MB)
        set(value) = prefs.edit().putInt(KEY_BUFFER_FLASH_MB, value.coerceAtLeast(0)).apply()

    /** Whether the user left the gateway running (so it is restored after the process is restarted). */
    var gatewayEnabled: Boolean
        get() = prefs.getBoolean(KEY_ENABLED, false)
        set(value) = prefs.edit().putBoolean(KEY_ENABLED, value).apply()

    private fun encrypt(plain: String): String? = runCatching {
        val cipher = Cipher.getInstance(TRANSFORMATION).apply { init(Cipher.ENCRYPT_MODE, secretKey()) }
        val sealed = cipher.iv + cipher.doFinal(plain.toByteArray(Charsets.UTF_8))
        Base64.encodeToString(sealed, Base64.NO_WRAP)
    }.onFailure { Log.w(TAG, "cannot encrypt ingest key; not persisting it: ${it.message}") }.getOrNull()

    private fun decrypt(stored: String): String? = runCatching {
        val sealed = Base64.decode(stored, Base64.NO_WRAP)
        val cipher = Cipher.getInstance(TRANSFORMATION).apply {
            init(Cipher.DECRYPT_MODE, secretKey(), GCMParameterSpec(128, sealed, 0, IV_BYTES))
        }
        String(cipher.doFinal(sealed, IV_BYTES, sealed.size - IV_BYTES), Charsets.UTF_8)
    }.onFailure { Log.w(TAG, "cannot decrypt stored ingest key: ${it.message}") }.getOrNull()

    private fun secretKey(): SecretKey {
        val keyStore = KeyStore.getInstance(ANDROID_KEYSTORE).apply { load(null) }
        (keyStore.getKey(KEY_ALIAS, null) as? SecretKey)?.let { return it }
        return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, ANDROID_KEYSTORE).apply {
            init(
                KeyGenParameterSpec.Builder(KEY_ALIAS, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
                    .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                    .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                    .setKeySize(256)
                    .build(),
            )
        }.generateKey()
    }

    private companion object {
        const val TAG = "IngestKeyStore"
        const val ANDROID_KEYSTORE = "AndroidKeyStore"
        const val KEY_ALIAS = "spotflow_ingest_key"
        const val TRANSFORMATION = "AES/GCM/NoPadding"
        const val IV_BYTES = 12
        const val KEY_INGEST = "ingest_key_enc"
        const val KEY_BUFFER_RAM_MB = "buffer_ram_mb"
        const val DEFAULT_BUFFER_RAM_MB = 1
        const val KEY_BUFFER_FLASH_MB = "buffer_flash_mb"
        const val DEFAULT_BUFFER_FLASH_MB = 50
        const val KEY_ENABLED = "gateway_enabled"
        const val LEGACY_KEY_INGEST = "ingest_key"
        const val LEGACY_SECURE_PREFS = "spotflow_secure_prefs"
    }
}

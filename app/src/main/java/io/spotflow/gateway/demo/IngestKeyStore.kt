package io.spotflow.gateway.demo

import android.content.Context
import android.content.SharedPreferences
import android.util.Log
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey

/**
 * Persists the Spotflow ingest key across app restarts.
 *
 * The key is a secret, so it is stored in [EncryptedSharedPreferences] (backed by the Android Keystore).
 * If encrypted storage cannot be initialised on a given device, it falls back to regular preferences so
 * the app still works — a reasonable trade-off for a reference demo.
 */
class IngestKeyStore(context: Context) {

    private val prefs: SharedPreferences = runCatching {
        val masterKey = MasterKey.Builder(context)
            .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
            .build()
        EncryptedSharedPreferences.create(
            context,
            "spotflow_secure_prefs",
            masterKey,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM,
        )
    }.getOrElse {
        Log.w("IngestKeyStore", "encrypted prefs unavailable, falling back: ${it.message}")
        context.getSharedPreferences("spotflow_prefs", Context.MODE_PRIVATE)
    }

    var ingestKey: String?
        get() = prefs.getString(KEY_INGEST, null)
        set(value) = prefs.edit().apply {
            if (value.isNullOrBlank()) remove(KEY_INGEST) else putString(KEY_INGEST, value)
        }.apply()

    /** Store-and-forward buffer size in megabytes (default 1, min 1). */
    var bufferMb: Int
        get() = prefs.getInt(KEY_BUFFER_MB, DEFAULT_BUFFER_MB)
        set(value) = prefs.edit().putInt(KEY_BUFFER_MB, value.coerceAtLeast(1)).apply()

    private companion object {
        const val KEY_INGEST = "ingest_key"
        const val KEY_BUFFER_MB = "buffer_mb"
        const val DEFAULT_BUFFER_MB = 1
    }
}

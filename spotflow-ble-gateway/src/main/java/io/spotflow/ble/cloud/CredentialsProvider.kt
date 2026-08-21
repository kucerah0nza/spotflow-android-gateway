package io.spotflow.ble.cloud

/**
 * Supplies the Spotflow ingest key used as the MQTT password for a given device.
 *
 * The key is resolved per device ID so hosts can rotate keys or provision a distinct key per device.
 * For the common case where a single workspace ingest key covers every device, use [StaticIngestKey].
 */
fun interface CredentialsProvider {
    /** Returns the ingest key (MQTT password) to use for [deviceId]. May suspend to fetch/refresh. */
    suspend fun ingestKey(deviceId: String): String
}

/** A [CredentialsProvider] that returns the same workspace ingest key for every device. */
class StaticIngestKey(private val key: String) : CredentialsProvider {
    override suspend fun ingestKey(deviceId: String): String = key
}

package io.spotflow.ble.cloud

/**
 * Supplies the Spotflow ingest key used as the MQTT password for a given device.
 *
 * The key is resolved per device ID, and again on every connection attempt, so hosts can rotate keys or
 * provision a distinct key per device. If the broker rejects a key, the gateway asks again after a while
 * (see `SpotflowGateway.retryStopped`). For the common case where a single workspace ingest key covers
 * every device, use [StaticIngestKey].
 */
public fun interface CredentialsProvider {
    /** Returns the ingest key (MQTT password) to use for [deviceId]. May suspend to fetch/refresh. */
    public suspend fun ingestKey(deviceId: String): String
}

/** A [CredentialsProvider] that returns the same workspace ingest key for every device. */
public class StaticIngestKey(private val key: String) : CredentialsProvider {
    override suspend fun ingestKey(deviceId: String): String = key
}

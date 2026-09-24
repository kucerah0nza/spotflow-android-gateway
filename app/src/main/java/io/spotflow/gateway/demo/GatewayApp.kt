package io.spotflow.gateway.demo

import android.app.Application
import io.spotflow.ble.SpotflowGateway
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.StaticIngestKey
import io.spotflow.ble.service.SpotflowGatewayService

/**
 * Re-installs the gateway service hooks whenever the process starts. If Android kills the process while
 * the gateway runs, the sticky service is recreated in a fresh process — where the hooks set by
 * [MainActivity] are gone — so they must be restored here for relaying to resume.
 */
class GatewayApp : Application() {
    override fun onCreate() {
        super.onCreate()
        val store = IngestKeyStore(this)
        val key = store.ingestKey
        if (store.gatewayEnabled && !key.isNullOrBlank()) {
            configureGatewayService(key, store.bufferRamMb, store.bufferFlashMb)
        }
    }
}

/** Points [SpotflowGatewayService] at a gateway built from these settings. */
fun configureGatewayService(key: String, ramMb: Int, flashMb: Int) {
    val ramBytes = ramMb.toLong() * 1024L * 1024L
    val flashBytes = flashMb.toLong() * 1024L * 1024L
    val config = MqttConfig(bufferMaxBytes = ramBytes + flashBytes, ramBufferMaxBytes = ramBytes)
    SpotflowGatewayService.gatewayFactory = { ctx -> SpotflowGateway(ctx, StaticIngestKey(key), config) }
    SpotflowGatewayService.onReady = { gateway ->
        runCatching { gateway.startScanning() } // SecurityException if BLE permissions were revoked
    }
}

package io.spotflow.ble.transport

import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.os.ParcelUuid
import android.os.SystemClock
import io.spotflow.ble.protocol.GattProfile
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow

/**
 * Scans for devices advertising the Spotflow Observability Service. Each device is emitted when first
 * seen and again at most every [REEMIT_INTERVAL_MS] while it keeps advertising, so a device the gateway
 * gave up on is picked up again once it is back in range.
 *
 * Requires `BLUETOOTH_SCAN` (API 31+) or `BLUETOOTH_ADMIN` + location (<= API 30). The returned flow
 * scans while collected and stops on cancellation.
 */
class SpotflowScanner(private val adapter: BluetoothAdapter) {

    @SuppressLint("MissingPermission")
    fun scan(): Flow<BluetoothDevice> = callbackFlow {
        val scanner = adapter.bluetoothLeScanner
            ?: throw IllegalStateException("BLE scanner unavailable (Bluetooth off?)")

        val lastEmitted = HashMap<String, Long>() // only touched on the (serial) scan callback thread
        val filter = ScanFilter.Builder()
            .setServiceUuid(ParcelUuid(GattProfile.SERVICE))
            .build()
        // BALANCED rather than LOW_LATENCY: this scan runs continuously for the life of the gateway, so
        // it must be power-friendly. Discovery of a new device just takes a little longer.
        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_BALANCED)
            .build()

        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, result: ScanResult) {
                val device = result.device
                val now = SystemClock.elapsedRealtime()
                val last = lastEmitted[device.address]
                if (last == null || now - last >= REEMIT_INTERVAL_MS) {
                    lastEmitted[device.address] = now
                    trySend(device)
                }
            }

            override fun onScanFailed(errorCode: Int) {
                close(IllegalStateException("BLE scan failed: $errorCode"))
            }
        }

        scanner.startScan(listOf(filter), settings, callback)
        awaitClose { runCatching { scanner.stopScan(callback) } }
    }

    private companion object {
        const val REEMIT_INTERVAL_MS = 30_000L
    }
}

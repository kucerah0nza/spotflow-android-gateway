package io.spotflow.ble.transport

import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.os.ParcelUuid
import io.spotflow.ble.protocol.GattProfile
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow

/**
 * Scans for devices advertising the Spotflow Observability Service and emits each distinct device once.
 *
 * Requires `BLUETOOTH_SCAN` (API 31+) or `BLUETOOTH_ADMIN` + location (<= API 30). The returned flow
 * scans while collected and stops on cancellation.
 */
class SpotflowScanner(private val adapter: BluetoothAdapter) {

    @SuppressLint("MissingPermission")
    fun scan(): Flow<BluetoothDevice> = callbackFlow {
        val scanner = adapter.bluetoothLeScanner
            ?: throw IllegalStateException("BLE scanner unavailable (Bluetooth off?)")

        val seen = HashSet<String>()
        val filter = ScanFilter.Builder()
            .setServiceUuid(ParcelUuid(GattProfile.SERVICE))
            .build()
        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()

        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, result: ScanResult) {
                val device = result.device
                if (seen.add(device.address)) {
                    trySend(device)
                }
            }

            override fun onScanFailed(errorCode: Int) {
                close(IllegalStateException("BLE scan failed: $errorCode"))
            }
        }

        scanner.startScan(listOf(filter), settings, callback)
        awaitClose { scanner.stopScan(callback) }
    }
}

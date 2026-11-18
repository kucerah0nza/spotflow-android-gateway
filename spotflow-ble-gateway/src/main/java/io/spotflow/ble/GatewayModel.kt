package io.spotflow.ble

import io.spotflow.ble.transport.ConnectionState

/** Observable state of one device being gatewayed, surfaced to the host app / demo UI. */
public data class GatewayDeviceState(
    val address: String,
    val deviceId: String? = null,
    val ble: ConnectionState = ConnectionState.DISCONNECTED,
    val cloudConnected: Boolean = false,
    /** Messages delivered to the cloud for this device since the gateway started (across reconnects). */
    val forwarded: Long = 0,
    /**
     * Messages received from the cloud for this device (desired configuration, e.g. a new minimal log
     * severity) since the gateway started, across reconnects — counted on arrival, before they are
     * written to the device.
     */
    val received: Long = 0,
    /**
     * Latest BLE signal strength in dBm (higher/closer to 0 is stronger), or null if not yet read. It is
     * only sampled while something collects [SpotflowGateway.devices].
     */
    val rssi: Int? = null,
    /** Messages buffered for this device, waiting to be uploaded (RAM and flash). */
    val pendingMessages: Long = 0,
    /** Bytes buffered in the in-memory tier (normal while briefly offline). */
    val ramBytes: Long = 0,
    /** Bytes spilled to the persistent (flash) tier (only after a longer outage). */
    val diskBytes: Long = 0,
    /** What currently keeps this device from being relayed, if anything. */
    val error: DeviceError? = null,
    /**
     * Whether the device takes OTA updates through this gateway: its Capabilities advertise them and the
     * gateway's [OtaConfig.enabled] is on. Known once the device has been read.
     */
    val otaEnabled: Boolean = false,
    /** The OTA artifact being transferred to the device, if any. */
    val otaTransfer: OtaTransfer? = null,
)

/** An OTA artifact on its way to a device through the gateway; see [GatewayDeviceState.otaTransfer]. */
public data class OtaTransfer(
    /** The artifact's size, or null while the gateway is still downloading it. */
    val sizeBytes: Long?,
    /** How much of it the device has read. */
    val deliveredBytes: Long,
)

/**
 * Over-the-air updates through the gateway (Spotflow BLE protocol 1.1). The gateway forwards a device's OTA
 * commands and results, and downloads its firmware over HTTPS on its behalf.
 *
 * The gateway is part of the update's trust boundary: it holds the artifacts' download credentials while
 * it serves them, and decides what bytes reach the device. Firmware signatures are still checked on the
 * device (by its bootloader or update handler).
 */
public data class OtaConfig(
    /** Whether to relay OTA updates. Off, OTA-capable devices are relayed for telemetry and configuration. */
    val enabled: Boolean = true,
    /** Largest artifact the gateway downloads; a device asking for a larger one is told it is too large. */
    val maxArtifactBytes: Long = 32L * 1024 * 1024,
)

/** Why a device is not (fully) relayed at the moment. */
public data class DeviceError(val kind: Kind, val message: String) {
    public enum class Kind {
        /**
         * The broker rejected the ingest key for this device. The gateway asks the credentials provider
         * again after a pause that grows to 15 minutes; `SpotflowGateway.retryStopped()` retries at once.
         */
        AUTH_REJECTED,

        /** The cloud could not be reached, or refused the connection for another reason. Retried. */
        CLOUD_UNREACHABLE,

        /** The Bluetooth session failed (connecting, discovering services, reading the device). Retried. */
        BLUETOOTH,

        /** Another connected device reports the same device ID; only one of them is relayed. Retried. */
        DUPLICATE_DEVICE,
    }
}

/**
 * Decides which devices the gateway relays. Called after the device ID has been read, before anything is
 * sent to the cloud under that ID.
 *
 * Managed mode connects to anything advertising the Spotflow service, and the BLE protocol does not
 * authenticate devices — so unless the filter is selective, any nearby peripheral can publish data (and
 * receive desired configuration) under any device ID in your workspace. Production integrations should
 * accept only the devices they expect (e.g. an allowlist, or IDs provisioned to the signed-in user).
 *
 * A rejected device is asked about again after a few minutes, and right away after
 * [SpotflowGateway.retryStopped] — call that when what the filter accepts has changed.
 */
public fun interface DeviceFilter {
    public fun accept(address: String, deviceId: String): Boolean

    public companion object {
        /**
         * Relays every device in range. Fine for a demo or a lab; in production it lets any nearby
         * peripheral use your ingest key (see [DeviceFilter]).
         */
        @JvmField
        public val AcceptAll: DeviceFilter = DeviceFilter { _, _ -> true }
    }
}

/** What the gateway's device discovery is doing; see [GatewayStatus.scan]. */
public enum class ScanState {
    /** Not scanning: [SpotflowGateway.startScanning] was not called, or scanning was stopped. */
    STOPPED,

    /** Scanning for devices advertising the Spotflow service. */
    SCANNING,

    /** Bluetooth is off; scanning resumes by itself when it is turned on. */
    WAITING_FOR_BLUETOOTH,

    /** The last scan failed (see [GatewayStatus.scanError]); it is being retried with backoff. */
    RETRYING,
}

/** Gateway-wide state that is not about any one device. */
public data class GatewayStatus(
    val scan: ScanState = ScanState.STOPPED,
    /** Why the scan failed, while [scan] is [ScanState.RETRYING] — e.g. a missing permission. */
    val scanError: String? = null,
    /** Whether the phone has a network. Without one, uploads wait instead of retrying. */
    val online: Boolean = true,
    /**
     * False if the flash tier of the buffer cannot be used (e.g. storage is full): the gateway still
     * relays, buffering in RAM only.
     */
    val storageAvailable: Boolean = true,
)

/** Thrown when [DeviceFilter] rejects a device (or it reports an unusable device ID). */
internal class DeviceRejectedException(message: String) : Exception(message)

/** Thrown when a second BLE session reports a device ID that already has a live session. */
internal class DuplicateDeviceException(deviceId: String) :
    IllegalStateException("device ID '$deviceId' is already connected via another Bluetooth address")

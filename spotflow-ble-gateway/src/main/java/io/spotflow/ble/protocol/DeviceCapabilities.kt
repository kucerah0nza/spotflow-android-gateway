package io.spotflow.ble.protocol

/**
 * The value of the Capabilities characteristic:
 *
 * ```
 * [0] protocol version major   [1] protocol version minor   [2] feature bits (bit 0: OTA supported)
 * ```
 *
 * Devices of protocol 1.0 expose only the major version (or major and minor without the feature byte);
 * missing bytes read as 0. More bytes may follow in later versions and are ignored.
 */
internal data class DeviceCapabilities(
    val major: Int,
    val minor: Int,
    val features: Int,
) {
    /** The device can take over-the-air updates through the gateway (protocol 1.1 and newer). */
    val otaSupported: Boolean get() = features and FEATURE_OTA != 0

    /** Whether this gateway speaks the device's protocol: same major version; minor versions only add. */
    val isCompatible: Boolean get() = major == SUPPORTED_MAJOR

    override fun toString(): String = "$major.$minor" + if (otaSupported) " (OTA)" else ""

    companion object {
        const val SUPPORTED_MAJOR = 1
        const val FEATURE_OTA = 0x01

        /** Parses the characteristic's value. Throws [IllegalArgumentException] if it is empty. */
        fun parse(value: ByteArray): DeviceCapabilities {
            require(value.isNotEmpty()) { "Capabilities characteristic is empty" }
            fun byte(index: Int) = value.getOrNull(index)?.toInt()?.and(0xFF) ?: 0
            return DeviceCapabilities(major = byte(0), minor = byte(1), features = byte(2))
        }
    }
}

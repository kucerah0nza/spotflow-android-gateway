package io.spotflow.ble.protocol

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class DeviceCapabilitiesTest {

    @Test
    fun `protocol 1_1 with OTA`() {
        val caps = DeviceCapabilities.parse(byteArrayOf(1, 1, 1))
        assertEquals(DeviceCapabilities(1, 1, 1), caps)
        assertTrue(caps.isCompatible)
        assertTrue(caps.otaSupported)
    }

    @Test
    fun `protocol 1_1 without OTA, and reserved feature bits`() {
        assertFalse(DeviceCapabilities.parse(byteArrayOf(1, 1, 0)).otaSupported)
        assertFalse(DeviceCapabilities.parse(byteArrayOf(1, 1, 0xFE.toByte())).otaSupported)
    }

    @Test
    fun `older devices expose fewer bytes`() {
        assertEquals(DeviceCapabilities(1, 0, 0), DeviceCapabilities.parse(byteArrayOf(1)))
        assertEquals(DeviceCapabilities(1, 0, 0), DeviceCapabilities.parse(byteArrayOf(1, 0)))
        assertTrue(DeviceCapabilities.parse(byteArrayOf(1)).isCompatible)
    }

    @Test
    fun `later bytes are ignored and another major version is not compatible`() {
        assertEquals(DeviceCapabilities(1, 2, 1), DeviceCapabilities.parse(byteArrayOf(1, 2, 1, 9, 9)))
        assertFalse(DeviceCapabilities.parse(byteArrayOf(2, 0, 0)).isCompatible)
    }

    @Test
    fun `an empty value is an error`() {
        assertThrows(IllegalArgumentException::class.java) { DeviceCapabilities.parse(ByteArray(0)) }
    }
}

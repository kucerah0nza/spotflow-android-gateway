package io.spotflow.ble.cloud

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class MqttUplinkTest {

    private fun desired(topic: String) = MqttUplink.isDesiredConfigurationTopic("config-cbor-c2d", topic)

    @Test
    fun `desired configuration topic matches the base and per-device topics below it`() {
        assertTrue(desired("config-cbor-c2d"))
        // What the Spotflow broker actually sends (workspace ID / device ID below the base topic).
        assertTrue(desired("config-cbor-c2d/01a0ce91-ef40-7abc-b7c2-85a0acfcd9ec/at-cc2340-0007"))
    }

    @Test
    fun `other topics are not desired configuration`() {
        assertFalse(desired("config-cbor-c2dx"))
        assertFalse(desired("config-cbor-d2c"))
        assertFalse(desired("ota-c2d/config-cbor-c2d"))
    }
}

package io.spotflow.ble

import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothProfile
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import io.mockk.verify
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.protocol.FrameCodec
import io.spotflow.ble.protocol.GattProfile
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.ConnectionState
import io.spotflow.ble.transport.FakePeripheral
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

/**
 * Attach mode end to end: the public [GattAttachment] driving the real GATT engine over a simulated
 * peripheral, with the host forwarding callbacks the way the contract asks.
 */
@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [26, 35]) // both GATT dialects: values through the characteristic (before Android 13), and as arguments
class GattAttachmentTest {

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val peripheral = FakePeripheral()
    private val address = "AA:BB:CC:DD:EE:FF"

    private fun attachTest(
        linkUp: Boolean = true,
        test: suspend TestScope.(gateway: SpotflowGateway, platform: FakePlatform, attachment: GattAttachment) -> Unit,
    ) = runTest {
        val platform = FakePlatform().apply { gattConnected = linkUp }
        val gateway = SpotflowGateway(
            context, MqttConfig(), DeviceFilter.AcceptAll, SpotflowGateway.DEFAULT_MTU, platform, StandardTestDispatcher(testScheduler),
        )
        try {
            test(gateway, platform, gateway.newAttachment())
        } finally {
            gateway.shutdown()
            advanceUntilIdle()
        }
    }

    /** The host forwards every callback its GATT delivers to the attachment. */
    private fun TestScope.forwardAll(attachment: GattAttachment) {
        runCurrent()
        while (peripheral.deliver(attachment.gattCallback)) runCurrent()
    }

    @Test
    fun `nothing touches the GATT until start is called`() = attachTest { _, _, attachment ->
        runCurrent()
        // Events arriving before start (the callback may already be wired into connectGatt) are harmless.
        attachment.gattCallback.onConnectionStateChange(peripheral.gatt, BluetoothGatt.GATT_SUCCESS, BluetoothProfile.STATE_CONNECTED)
        runCurrent()

        verify(exactly = 0) { peripheral.gatt.discoverServices() }
    }

    @Test
    fun `an attached device is relayed, in protocol order`() = attachTest { gateway, platform, attachment ->
        attachment.start(peripheral.gatt)
        forwardAll(attachment)

        val state = gateway.devices.value[address]
        assertEquals(ConnectionState.READY, state?.ble)
        assertEquals("attached-device", state?.deviceId)
        assertArrayEquals("the stream is enabled last", BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE, peripheral.cccdWrites.single())
        assertTrue(platform.uplink("attached-device").publishedTo("ingest-cbor", "meta".toByteArray()))

        FrameCodec.fragment(MessageType.TELEMETRY, byteArrayOf(4, 5, 6), seq = 1, mtu = 247).forEach {
            peripheral.notify(attachment.gattCallback, peripheral.tx, it)
        }
        runCurrent()
        assertTrue(platform.uplink("attached-device").publishedTo("ingest-cbor", byteArrayOf(4, 5, 6)))
        verify(exactly = 0) { peripheral.gatt.readRemoteRssi() } // the host's GATT: no polling unless asked
    }

    @Test
    fun `a GATT handed over while still connecting is waited for`() = attachTest(linkUp = false) { gateway, _, attachment ->
        // The host passed gattCallback straight to connectGatt and calls start() right away.
        attachment.start(peripheral.gatt)
        runCurrent()
        assertEquals(ConnectionState.CONNECTING, gateway.devices.value[address]?.ble)
        verify(exactly = 0) { peripheral.gatt.discoverServices() }

        attachment.gattCallback.onConnectionStateChange(peripheral.gatt, BluetoothGatt.GATT_SUCCESS, BluetoothProfile.STATE_CONNECTED)
        forwardAll(attachment)

        assertEquals(ConnectionState.READY, gateway.devices.value[address]?.ble)
    }

    @Test
    fun `detaching turns the device's stream off and leaves the link to the host`() = attachTest { gateway, _, attachment ->
        attachment.start(peripheral.gatt)
        forwardAll(attachment)

        attachment.detach()
        forwardAll(attachment)

        assertArrayEquals(BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE, peripheral.cccdWrites.last())
        verify(exactly = 0) { peripheral.gatt.disconnect() }
        verify(exactly = 0) { peripheral.gatt.close() }
        assertNull(gateway.devices.value[address])
    }

    @Test
    fun `the host's own operations wait their turn inside runExclusive`() = attachTest { _, _, attachment ->
        attachment.start(peripheral.gatt)
        runCurrent() // the gateway's service discovery is now outstanding

        var hostRan = false
        val hostOperation = launch { attachment.runExclusive { hostRan = true } }
        runCurrent()
        assertFalse("the gateway's operation is in flight", hostRan)

        forwardAll(attachment)
        assertTrue(hostRan)
        assertTrue(hostOperation.isCompleted)
    }

    @Test
    fun `after the link drops, starting again relays the device over the same callback`() =
        attachTest { gateway, platform, attachment ->
            attachment.start(peripheral.gatt)
            forwardAll(attachment)

            attachment.gattCallback.onConnectionStateChange(peripheral.gatt, 8, BluetoothProfile.STATE_DISCONNECTED)
            runCurrent()
            assertEquals(ConnectionState.DISCONNECTED, gateway.devices.value[address]?.ble)

            attachment.start(peripheral.gatt) // the host reconnected its GATT
            forwardAll(attachment)

            assertEquals(ConnectionState.READY, gateway.devices.value[address]?.ble)
            assertEquals("the cloud connection was kept meanwhile", 1, platform.uplink("attached-device").connectCount)
        }

    @Test
    fun `starting with another GATT ends the first device's session and keeps their events apart`() =
        attachTest { gateway, platform, attachment ->
            attachment.start(peripheral.gatt)
            forwardAll(attachment)

            val otherAddress = "11:22:33:44:55:66"
            val other = FakePeripheral(otherAddress).apply { values[GattProfile.DEVICE_ID] = "other-device".toByteArray() }
            attachment.start(other.gatt)
            runCurrent()
            while (peripheral.deliver(attachment.gattCallback) || other.deliver(attachment.gattCallback)) runCurrent()

            assertNull("the first device is no longer relayed", gateway.devices.value[address])
            assertArrayEquals("and was told to stop streaming", BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE, peripheral.cccdWrites.last())
            assertEquals(ConnectionState.READY, gateway.devices.value[otherAddress]?.ble)

            // A late notification from the first device must not be uploaded as the second one's.
            FrameCodec.fragment(MessageType.TELEMETRY, byteArrayOf(6, 6, 6), seq = 2, mtu = 247).forEach {
                peripheral.notify(attachment.gattCallback, peripheral.tx, it)
            }
            runCurrent()
            assertFalse(platform.uplink("other-device").publishedTo("ingest-cbor", byteArrayOf(6, 6, 6)))
        }

    @Test
    fun `starting again on the same GATT winds the old session down without a stall`() =
        attachTest { gateway, _, attachment ->
            attachment.start(peripheral.gatt)
            forwardAll(attachment)

            attachment.start(peripheral.gatt)
            forwardAll(attachment) // no time passes: the old session gets the answer to its own request

            assertEquals(ConnectionState.READY, gateway.devices.value[address]?.ble)
            assertEquals(0L, testScheduler.currentTime)
        }

    @Test
    fun `runExclusive before start says so`() = attachTest { _, _, attachment ->
        val error = runCatching { attachment.runExclusive { } }.exceptionOrNull()
        assertTrue(error is IllegalStateException)
    }
}

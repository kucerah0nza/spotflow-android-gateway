package io.spotflow.ble

import android.bluetooth.BluetoothDevice
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import io.spotflow.ble.cloud.MessageStore
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.Uplink
import io.spotflow.ble.protocol.Message
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.ConnectionState
import io.spotflow.ble.transport.DeviceUnreachableException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner

/**
 * The gateway's own logic — discovery, the per-device reconnect loop, what it reports — against a fake
 * [GatewayPlatform], in virtual time.
 */
@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
class SpotflowGatewayTest {

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val address = "AA:BB:CC:DD:EE:FF"
    private val telemetry = Message(MessageType.TELEMETRY, byteArrayOf(1, 2, 3))

    /**
     * Runs [test] with a gateway on the test scheduler, and shuts the gateway down afterwards so the
     * process-wide flash store is closed before the next test gets a fresh data directory.
     */
    private fun gatewayTest(
        filter: DeviceFilter = DeviceFilter.AcceptAll,
        test: suspend TestScope.(gateway: SpotflowGateway, platform: FakePlatform) -> Unit,
    ) = runTest {
        val platform = FakePlatform()
        val gateway = gateway(platform, filter)
        try {
            test(gateway, platform)
        } finally {
            gateway.shutdown()
            advanceUntilIdle()
        }
    }

    private fun TestScope.gateway(platform: FakePlatform, filter: DeviceFilter = DeviceFilter.AcceptAll) =
        SpotflowGateway(
            context, MqttConfig(), filter, SpotflowGateway.DEFAULT_MTU, platform, StandardTestDispatcher(testScheduler),
        )

    private fun SpotflowGateway.state(): GatewayDeviceState? = devices.value[address]

    // ---- discovery -------------------------------------------------------------------------------

    @Test
    fun `a failed scan is retried with backoff and reported meanwhile`() = gatewayTest { gateway, platform ->
        platform.failingScans = 2
        gateway.startScanning()
        runCurrent()

        assertEquals(ScanState.RETRYING, gateway.status.value.scan)
        assertNotNull(gateway.status.value.scanError)

        advanceTimeBy(5_001); runCurrent() // the second attempt fails too
        assertEquals(ScanState.RETRYING, gateway.status.value.scan)
        advanceTimeBy(10_001); runCurrent() // the third works

        assertEquals(3, platform.scans.size)
        assertEquals(ScanState.SCANNING, gateway.status.value.scan)
        assertNull(gateway.status.value.scanError)
    }

    @Test
    fun `a scan that dies while running is restarted`() = gatewayTest { gateway, platform ->
        gateway.startScanning()
        runCurrent()

        platform.failRunningScan(IllegalStateException("BLE scan failed (error 3)"))
        runCurrent()
        assertEquals(ScanState.RETRYING, gateway.status.value.scan)

        advanceTimeBy(5_001); runCurrent()
        assertEquals(ScanState.SCANNING, gateway.status.value.scan)
        assertEquals(2, platform.scans.size)
    }

    @Test
    fun `the scan starts quick, then runs low-power and is restarted before Android throttles it`() =
        gatewayTest { gateway, platform ->
            gateway.startScanning()
            runCurrent()
            assertEquals(listOf(false), platform.scans)

            advanceTimeBy(2 * 60_000L + 1); runCurrent()
            assertEquals(listOf(false, true), platform.scans)

            advanceTimeBy(25 * 60_000L + 1); runCurrent()
            assertEquals("restarted inside Android's 30-minute limit", listOf(false, true, true), platform.scans)
            assertEquals(ScanState.SCANNING, gateway.status.value.scan)
        }

    @Test
    fun `scanning waits for Bluetooth and resumes when it is turned on`() = gatewayTest { gateway, platform ->
        platform.isBluetoothEnabled = false
        gateway.startScanning()
        runCurrent()
        assertEquals(ScanState.WAITING_FOR_BLUETOOTH, gateway.status.value.scan)
        assertTrue(platform.scans.isEmpty())

        platform.turnBluetoothOn()
        runCurrent()
        assertEquals(ScanState.SCANNING, gateway.status.value.scan)

        gateway.stopScanning()
        assertEquals(ScanState.STOPPED, gateway.status.value.scan)
    }

    // ---- the managed reconnect loop --------------------------------------------------------------

    @Test
    fun `a discovered device is relayed, and reconnected directly after a drop`() = gatewayTest { gateway, platform ->
        val device = platform.device(address)
        gateway.startScanning()
        runCurrent()
        platform.advertise(device)
        runCurrent()

        val first = device.attempts.single()
        assertFalse("found by the scan, so in range: a direct connect", first.autoConnect)
        assertEquals(ConnectionState.READY, gateway.state()?.ble)
        assertEquals("test-device", gateway.state()?.deviceId)

        first.connection.emit(telemetry)
        runCurrent()
        assertTrue(platform.uplink("test-device").publishedTo("ingest-cbor", byteArrayOf(1, 2, 3)))
        assertEquals(true, gateway.state()?.cloudConnected)

        first.connection.drop()
        runCurrent()
        assertEquals(ConnectionState.DISCONNECTED, gateway.state()?.ble)
        advanceTimeBy(501); runCurrent()

        assertEquals(2, device.attempts.size)
        assertFalse("a working session just dropped: try a direct connect first", device.attempts[1].autoConnect)
        assertEquals(ConnectionState.READY, gateway.state()?.ble)
        assertEquals("the cloud connection was kept across the drop", 1, platform.uplink("test-device").connectCount)
    }

    @Test
    fun `after a failed attempt it waits with autoConnect, and a device that stays away is let go`() =
        gatewayTest { gateway, platform ->
            val device = platform.device(address)
            device.failures += IllegalStateException("connection failed, status=133")
            device.failures += DeviceUnreachableException("not seen for 600s")
            gateway.startScanning()
            runCurrent()
            platform.advertise(device)
            runCurrent()

            assertEquals(DeviceError.Kind.BLUETOOTH, gateway.state()?.error?.kind)
            assertEquals(ConnectionState.DISCONNECTED, gateway.state()?.ble)

            advanceTimeBy(2_001); runCurrent()
            assertEquals(2, device.attempts.size)
            assertTrue("the device may be away: let Android re-attach when it is back", device.attempts[1].autoConnect)
            assertNull("gone for good: no longer listed", gateway.state())

            platform.advertise(device) // it is back after all
            runCurrent()
            assertEquals(3, device.attempts.size)
            assertEquals(ConnectionState.READY, gateway.state()?.ble)
        }

    @Test
    fun `a rejected key is retried after a pause, with the credentials asked for anew`() = gatewayTest { gateway, platform ->
        val device = platform.device(address)
        platform.uplink("test-device").authFail = true
        gateway.startScanning()
        runCurrent()
        platform.advertise(device)
        runCurrent()

        assertEquals(DeviceError.Kind.AUTH_REJECTED, gateway.state()?.error?.kind)
        assertEquals(ConnectionState.DISCONNECTED, gateway.state()?.ble)
        advanceTimeBy(30_000); runCurrent()
        assertEquals("not hammering the broker", 1, platform.uplink("test-device").connectAttempts)

        platform.uplink("test-device").authFail = false // the key was fixed
        advanceTimeBy(30_001); runCurrent()

        assertEquals(2, device.attempts.size)
        assertEquals(true, gateway.state()?.cloudConnected)
        assertNull(gateway.state()?.error)
    }

    @Test
    fun `retryStopped ends the pause after a rejected key`() = gatewayTest { gateway, platform ->
        val device = platform.device(address)
        platform.uplink("test-device").authFail = true
        gateway.startScanning()
        runCurrent()
        platform.advertise(device)
        runCurrent()
        assertEquals(DeviceError.Kind.AUTH_REJECTED, gateway.state()?.error?.kind)

        platform.uplink("test-device").authFail = false
        gateway.retryStopped()
        runCurrent() // no time passes

        assertEquals(true, gateway.state()?.cloudConnected)
        assertNull(gateway.state()?.error)
    }

    @Test
    fun `a device the filter rejects is hidden, left alone, and asked about again later`() {
        val accepted = HashSet<String>()
        gatewayTest(filter = { _, deviceId -> deviceId in accepted }) { gateway, platform ->
            val device = platform.device(address)
            gateway.startScanning()
            runCurrent()
            platform.advertise(device)
            runCurrent()

            assertNull("not ours: not listed", gateway.state())
            assertTrue("and disconnected again", device.attempts.single().connection.closed)
            assertEquals(0, platform.uplink("test-device").connectAttempts)

            platform.advertise(device) // it keeps advertising
            advanceTimeBy(60_000); runCurrent()
            assertEquals("left alone for a while", 1, device.attempts.size)

            accepted += "test-device" // e.g. the user who owns it signed in
            gateway.retryStopped()
            runCurrent()
            platform.advertise(device)
            runCurrent()

            assertEquals(2, device.attempts.size)
            assertEquals(ConnectionState.READY, gateway.state()?.ble)
        }
    }

    @Test
    fun `a rejected device is looked at afresh after ten minutes by itself`() {
        val accepted = HashSet<String>()
        gatewayTest(filter = { _, deviceId -> deviceId in accepted }) { gateway, platform ->
            val device = platform.device(address)
            gateway.startScanning()
            runCurrent()
            platform.advertise(device)
            runCurrent()
            accepted += "test-device"

            advanceTimeBy(10 * 60_000L + 1); runCurrent()
            platform.advertise(device)
            runCurrent()

            assertEquals(ConnectionState.READY, gateway.state()?.ble)
        }
    }

    @Test
    fun `a second device reporting the same id is refused and says why`() = gatewayTest { gateway, platform ->
        val first = platform.device(address)
        val impostor = platform.device("11:22:33:44:55:66")
        gateway.startScanning()
        runCurrent()
        platform.advertise(first)
        runCurrent()
        platform.advertise(impostor)
        runCurrent()

        assertEquals(ConnectionState.READY, gateway.state()?.ble)
        assertEquals(DeviceError.Kind.DUPLICATE_DEVICE, gateway.devices.value["11:22:33:44:55:66"]?.error?.kind)
    }

    @Test
    fun `turning Bluetooth off tears sessions down, and they come back with it`() = gatewayTest { gateway, platform ->
        val device = platform.device(address)
        gateway.startScanning()
        runCurrent()
        platform.advertise(device)
        runCurrent()
        assertEquals(ConnectionState.READY, gateway.state()?.ble)

        platform.turnBluetoothOff()
        runCurrent()
        assertEquals(ConnectionState.DISCONNECTED, gateway.state()?.ble)
        assertEquals(ScanState.WAITING_FOR_BLUETOOTH, gateway.status.value.scan)
        assertTrue("the GATT was released", device.attempts.single().connection.closed)
        advanceTimeBy(60_000); runCurrent()
        assertEquals("nothing is attempted while Bluetooth is off", 1, device.attempts.size)

        platform.turnBluetoothOn()
        runCurrent()
        platform.advertise(device)
        runCurrent()
        assertEquals(ScanState.SCANNING, gateway.status.value.scan)
        assertEquals(ConnectionState.READY, gateway.state()?.ble)
    }

    @Test
    fun `detach stops relaying a device and removes it`() = gatewayTest { gateway, platform ->
        val device = platform.device(address)
        gateway.startScanning()
        runCurrent()
        platform.advertise(device)
        runCurrent()

        gateway.detach(address)
        runCurrent()

        assertNull(gateway.state())
        assertTrue(device.attempts.single().connection.closed)
    }

    @Test
    fun `a detached device is left alone by the scan until retryStopped`() = gatewayTest { gateway, platform ->
        val device = platform.device(address)
        gateway.startScanning()
        runCurrent()
        platform.advertise(device)
        runCurrent()
        gateway.detach(address)
        runCurrent()

        platform.advertise(device) // it advertises again as soon as it is disconnected
        advanceTimeBy(60_000); runCurrent()
        assertEquals("detached means detached", 1, device.attempts.size)
        assertNull(gateway.state())

        gateway.retryStopped()
        platform.advertise(device)
        runCurrent()
        assertEquals(ConnectionState.READY, gateway.state()?.ble)
    }

    @Test
    fun `stored data whose upload gave up is tried again on retryStopped`() = runTest {
        // Left on flash by an earlier run, for a device that is not coming back.
        val store = MessageStore.acquire(context, 10_000)
        store.enqueueAll("gone-device", listOf(MessageStore.Entry(1, "ingest-cbor", byteArrayOf(5, 5))))
        MessageStore.release(store)

        val platform = FakePlatform().apply { uplink("gone-device").authFail = true }
        val gateway = gateway(platform)
        try {
            advanceTimeBy(60_000); runCurrent()
            assertEquals("the key was rejected: the upload gave up", 1, platform.uplink("gone-device").connectAttempts)

            platform.uplink("gone-device").authFail = false // the key was fixed
            gateway.retryStopped()
            runCurrent()

            assertTrue(platform.uplink("gone-device").publishedTo("ingest-cbor", byteArrayOf(5, 5)))
        } finally {
            gateway.shutdown()
            advanceUntilIdle()
        }
    }

    // ---- status ----------------------------------------------------------------------------------

    @Test
    fun `the status follows connectivity`() = gatewayTest { gateway, platform ->
        runCurrent()
        assertTrue(gateway.status.value.online)
        assertTrue(gateway.status.value.storageAvailable)

        platform.online.value = false
        runCurrent()
        assertFalse(gateway.status.value.online)
    }

    @Test
    fun `signal strength is sampled only while the devices are being observed`() = gatewayTest { gateway, platform ->
        val device = platform.device(address)
        gateway.startScanning()
        runCurrent()
        platform.advertise(device)
        advanceTimeBy(30_000); runCurrent()
        val connection = device.attempts.single().connection
        assertEquals(0, connection.rssiReads)
        assertNull(gateway.state()?.rssi)

        val observer = backgroundScope.launch { gateway.devices.collect { } }
        advanceTimeBy(6_000); runCurrent()
        assertEquals(-55, gateway.state()?.rssi)

        observer.cancel()
        runCurrent()
        val reads = connection.rssiReads
        advanceTimeBy(30_000); runCurrent()
        assertTrue("stops soon after the observer leaves", connection.rssiReads <= reads + 1)
    }

    // ---- attach mode -----------------------------------------------------------------------------

    @Test
    fun `an attached connection is relayed until the host detaches it`() = gatewayTest { gateway, platform ->
        val connection = FakeBleConnection(address)
        gateway.startAttached(connection, pollRssi = false)
        runCurrent()
        assertEquals(ConnectionState.READY, gateway.state()?.ble)

        connection.emit(telemetry)
        runCurrent()
        assertTrue(platform.uplink("test-device").publishedTo("ingest-cbor", byteArrayOf(1, 2, 3)))

        gateway.detach(address)
        runCurrent()
        assertNull(gateway.state())
        assertTrue(connection.closed)
    }

    @Test
    fun `starting an attachment again replaces the previous session for that device`() = gatewayTest { gateway, platform ->
        val first = FakeBleConnection(address)
        gateway.startAttached(first, pollRssi = false)
        runCurrent()

        val second = FakeBleConnection(address)
        gateway.startAttached(second, pollRssi = false)
        runCurrent()

        assertTrue(first.closed)
        assertEquals(ConnectionState.READY, gateway.state()?.ble)
        assertNull("not refused as a duplicate of itself", gateway.state()?.error)
    }

    @Test
    fun `once an attached session is over, the scan may pick the device up`() = gatewayTest { gateway, platform ->
        val attached = FakeBleConnection(address)
        gateway.startAttached(attached, pollRssi = false)
        runCurrent()
        attached.drop() // the host's link went down, and the host does not attach again
        runCurrent()

        val device = platform.device(address)
        gateway.startScanning()
        runCurrent()
        platform.advertise(device)
        runCurrent()

        assertEquals("not blocked by the finished attached session", 1, device.attempts.size)
        assertEquals(ConnectionState.READY, gateway.state()?.ble)
    }

    // ---- restart ---------------------------------------------------------------------------------

    @Test
    fun `data a shut-down gateway writes out after its successor started is still uploaded`() = runTest {
        val oldPlatform = FakePlatform().apply { uplink("test-device").failConnect = true } // the cloud is unreachable
        val old = gateway(oldPlatform)
        val device = oldPlatform.device(address)
        old.startScanning()
        runCurrent()
        oldPlatform.advertise(device)
        runCurrent()
        device.attempts.single().connection.emit(Message(MessageType.TELEMETRY, byteArrayOf(9, 9)))
        runCurrent() // buffered in RAM only: nothing is on flash yet

        // Restart in place: the new gateway is created first and looks for stored buffers — finding none.
        val newPlatform = FakePlatform()
        val new = gateway(newPlatform)
        try {
            runCurrent()
            assertEquals(0, newPlatform.uplink("test-device").connectAttempts)

            old.shutdown() // only now does the old one write its RAM to flash
            runCurrent()

            assertTrue(
                "the device never reconnects, yet its data is delivered",
                newPlatform.uplink("test-device").publishedTo("ingest-cbor", byteArrayOf(9, 9)),
            )
        } finally {
            new.shutdown()
            advanceUntilIdle()
        }
    }
}

/** A [GatewayPlatform] under the test's control. */
internal class FakePlatform : GatewayPlatform {
    override var isBluetoothEnabled = true
    override val online = MutableStateFlow(true)

    private var onBluetoothOff: (() -> Unit)? = null
    private var onBluetoothOn: (() -> Unit)? = null

    override fun start(onBluetoothOff: () -> Unit, onBluetoothOn: () -> Unit) {
        this.onBluetoothOff = onBluetoothOff
        this.onBluetoothOn = onBluetoothOn
    }

    override fun stop() {
        onBluetoothOff = null
        onBluetoothOn = null
    }

    fun turnBluetoothOff() {
        isBluetoothEnabled = false
        onBluetoothOff?.invoke()
    }

    fun turnBluetoothOn() {
        isBluetoothEnabled = true
        onBluetoothOn?.invoke()
    }

    /** One entry per scan started: whether it was low-power. */
    val scans = ArrayList<Boolean>()

    /** The next this-many scans fail to start. */
    var failingScans = 0

    private val advertisements = Channel<Any>(Channel.UNLIMITED)

    override fun scan(lowPower: Boolean): Flow<DiscoveredDevice> = flow {
        scans += lowPower
        if (failingScans > 0) {
            failingScans--
            throw IllegalStateException("BLE scan failed (error 2)")
        }
        for (event in advertisements) {
            if (event is Throwable) throw event
            emit((event as FakeDevice).discovered)
        }
    }

    fun advertise(device: FakeDevice) {
        advertisements.trySend(device)
    }

    /** Makes the scan that is running fail, as `onScanFailed` would. */
    fun failRunningScan(error: Throwable) {
        advertisements.trySend(error)
    }

    fun device(address: String, deviceId: String = "test-device") = FakeDevice(address, deviceId)

    private val uplinks = HashMap<String, FakeUplink>()

    /** The one fake uplink of [deviceId], shared by every cloud link the gateway creates for it. */
    fun uplink(deviceId: String): FakeUplink = uplinks.getOrPut(deviceId) { FakeUplink() }

    override fun createUplink(deviceId: String): Uplink = uplink(deviceId)

    /** Whether an attached GATT's link is up. */
    var gattConnected = true

    override fun isGattConnected(device: BluetoothDevice): Boolean = gattConnected
}

/** A peripheral: every connection attempt the gateway makes to it is recorded. */
internal class FakeDevice(val address: String, private val deviceId: String) {
    class Attempt(val autoConnect: Boolean, val connection: FakeBleConnection)

    val attempts = ArrayList<Attempt>()

    /** Consumed one per attempt: that attempt fails with it. */
    val failures = ArrayDeque<Throwable>()

    val discovered = DiscoveredDevice(address) { autoConnect ->
        FakeBleConnection(address, deviceId).also { connection ->
            connection.prepareFailure = failures.removeFirstOrNull()
            attempts += Attempt(autoConnect, connection)
        }
    }
}

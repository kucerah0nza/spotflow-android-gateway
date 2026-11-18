package io.spotflow.ble

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import io.spotflow.ble.cloud.MqttAuthException
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.BufferPool
import io.spotflow.ble.cloud.MessageStore
import io.spotflow.ble.cloud.Uplink
import io.spotflow.ble.cloud.UplinkDisconnectedException
import io.spotflow.ble.cloud.UplinkThrottledException
import io.spotflow.ble.ota.ArtifactCache
import io.spotflow.ble.protocol.DeviceCapabilities
import io.spotflow.ble.protocol.Message
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import java.io.File

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
class GatewaySessionTest {

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val config = MqttConfig()

    @Before
    fun setUp() {
        context.databaseList().forEach { context.deleteDatabase(it) }
    }

    /** The flash store, opened after setUp() cleared the databases. */
    private val store by lazy { MessageStore(context, 10_000) }

    private fun registry(
        scope: CoroutineScope,
        uplink: FakeUplink,
        online: StateFlow<Boolean> = MutableStateFlow(true),
    ): CloudLinkRegistry {
        val pool = BufferPool(store, ramMaxBytes = 1_000, flashMaxBytes = 10_000)
        return CloudLinkRegistry(scope) { deviceId, push ->
            CloudLink(deviceId, pool.open(deviceId), uplink, push, online)
        }
    }

    /** Leaves [payload] on flash for [deviceId], as a run that was killed during an outage would. */
    private fun leaveOnFlash(deviceId: String, payload: ByteArray) {
        store.enqueueAll(deviceId, listOf(MessageStore.Entry(1, "ingest-cbor", payload)))
    }

    /** Mirrors the gateway: one status entry per address, shared by every session and cloud link. */
    private val board = HashMap<String, GatewayDeviceState>()

    private fun TestScope.session(
        ble: FakeBleConnection,
        uplink: FakeUplink,
        links: CloudLinkRegistry = registry(backgroundScope, uplink),
        filter: DeviceFilter = DeviceFilter.AcceptAll,
        rssiWanted: StateFlow<Boolean>? = null,
        artifacts: ArtifactCache? = null,
        onStatus: (GatewayDeviceState) -> Unit = {},
    ): GatewaySession {
        val address = ble.deviceAddress
        val push: StatusPush = { change ->
            val state = synchronized(board) {
                change(board[address] ?: GatewayDeviceState(address)).also { board[address] = it }
            }
            onStatus(state)
        }
        return GatewaySession(ble, config, links, push, filter, rssiWanted, artifacts)
    }

    // ---- OTA -----------------------------------------------------------------------------------------

    private val otaDevice get() = FakeBleConnection().apply { capabilities = DeviceCapabilities(1, 1, 0x01) }

    /** Serves 1000 bytes of firmware for any URL. */
    private fun TestScope.artifacts() = ArtifactCache(
        { File(context.cacheDir, "ota-test") },
        { _, _, _, target -> target.writeBytes(ByteArray(1000) { it.toByte() }); 1000L },
        maxArtifactBytes = 1_000_000,
        scope = backgroundScope,
    )

    private fun TestScope.otaSession(ble: FakeBleConnection, uplink: FakeUplink, links: CloudLinkRegistry = registry(backgroundScope, uplink), onStatus: (GatewayDeviceState) -> Unit = {}) =
        launch { runCatching { session(ble, uplink, links, artifacts = artifacts(), onStatus = onStatus).run() } }

    @Test
    fun `an OTA device's OTA commands are subscribed to once its session metadata is out`() = runTest {
        val ble = otaDevice
        val uplink = FakeUplink()
        var status: GatewayDeviceState? = null
        val job = otaSession(ble, uplink) { status = it }
        runCurrent()

        val (call, publishedBefore) = uplink.otaSubscriptionCalls.single()
        assertEquals("subscribe", call)
        assertTrue("after the metadata", publishedBefore >= 1)
        assertArrayEquals("meta".toByteArray(), uplink.published.first().second)
        assertTrue(status!!.otaEnabled)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `OTA results go to their topic`() = runTest {
        val ble = otaDevice
        val uplink = FakeUplink()
        val job = otaSession(ble, uplink)
        runCurrent()

        ble.emit(Message(MessageType.OTA_D2C, byteArrayOf(0x55)))
        runCurrent()

        assertTrue(uplink.publishedTo("ota-cbor-d2c", byteArrayOf(0x55)))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `a device without OTA, or a gateway with OTA off, gets no OTA at all`() = runTest {
        for ((ble, cache) in listOf(FakeBleConnection() to artifacts(), otaDevice to null)) {
            val uplink = FakeUplink()
            var status: GatewayDeviceState? = null
            val job = launch { runCatching { session(ble, uplink, artifacts = cache) { status = it }.run() } }
            runCurrent()

            ble.emit(Message(MessageType.OTA_D2C, byteArrayOf(0x55)))
            ble.emit(Message(MessageType.ARTIFACT_REQUEST, ByteArray(15)))
            runCurrent()

            assertTrue(uplink.otaSubscriptionCalls.isEmpty())
            assertFalse(uplink.published.any { it.first == "ota-cbor-d2c" })
            assertTrue(ble.sentArtifactResponses.isEmpty())
            assertFalse(status!!.otaEnabled)
            ble.drop(); runCurrent(); job.cancel()
        }
    }

    @Test
    fun `an OTA command is acknowledged to the broker only once the device has acknowledged it`() = runTest {
        val ble = otaDevice.apply { otaAck = CompletableDeferred() }
        val uplink = FakeUplink()
        val job = otaSession(ble, uplink)
        runCurrent()

        val acks = mutableListOf<Int>()
        uplink.otaCommandHandler!!(byteArrayOf(1)) { acks += 1 }
        uplink.otaCommandHandler!!(byteArrayOf(1)) { acks += 11 } // the broker's redelivery
        uplink.otaCommandHandler!!(byteArrayOf(2)) { acks += 2 }
        runCurrent()
        assertTrue(acks.isEmpty())

        ble.otaAck!!.complete(Unit); runCurrent()
        assertEquals(listOf(listOf<Byte>(1), listOf<Byte>(2)), ble.sentOtaCommands.map { it.toList() })
        assertEquals(listOf(1, 11, 2), acks)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `an OTA command not acknowledged by the device waits for its next session`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        val first = otaDevice.apply { otaAck = CompletableDeferred() }
        val job1 = otaSession(first, uplink, links)
        runCurrent()
        var acked = false
        uplink.otaCommandHandler!!(byteArrayOf(3)) { acked = true }
        runCurrent()
        first.otaAck!!.completeExceptionally(java.io.IOException("link lost"))
        first.drop(); runCurrent(); job1.cancel()
        assertFalse(acked)

        val second = otaDevice
        val job2 = otaSession(second, uplink, links)
        runCurrent()
        assertEquals(listOf<Byte>(3), second.sentOtaCommands.single().toList())
        assertTrue(acked)
        second.drop(); runCurrent(); job2.cancel()
    }

    @Test
    fun `every OTA session subscribes again, so the cloud replays an unfinished update`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        val first = otaDevice
        val job1 = otaSession(first, uplink, links)
        runCurrent()
        first.drop(); runCurrent(); job1.cancel()
        // The device rebooted; the link (and its MQTT connection) outlived the BLE session.
        val second = otaDevice
        val job2 = otaSession(second, uplink, links)
        runCurrent()
        uplink.loseConnection()
        advanceTimeBy(60_000); runCurrent()

        assertEquals(2, uplink.connectCount)
        assertEquals("both sessions, and the new connection", 3, uplink.otaSubscriptionCalls.count { it.first == "subscribe" })
        second.drop(); runCurrent(); job2.cancel()
    }

    @Test
    fun `a device that no longer takes OTA updates is unsubscribed`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        val first = otaDevice
        val job1 = otaSession(first, uplink, links)
        runCurrent()
        first.drop(); runCurrent(); job1.cancel()

        val reflashed = FakeBleConnection()
        val job2 = otaSession(reflashed, uplink, links)
        runCurrent()

        assertEquals(listOf("subscribe", "unsubscribe"), uplink.otaSubscriptionCalls.map { it.first })
        reflashed.drop(); runCurrent(); job2.cancel()
    }

    @Test
    fun `artifact requests are answered over BLE, with the transfer shown in the device's status`() = runTest {
        val ble = otaDevice
        val uplink = FakeUplink()
        var status: GatewayDeviceState? = null
        val job = otaSession(ble, uplink) { status = it }
        runCurrent()

        val url = "https://x".toByteArray()
        val open = java.nio.ByteBuffer.allocate(12 + url.size + 1).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .put(1).putInt(5).putInt(1).putShort(url.size.toShort()).put(1).put(url).put('s'.code.toByte()).array()
        ble.emit(Message(MessageType.ARTIFACT_REQUEST, open))
        runCurrent()
        val read = java.nio.ByteBuffer.allocate(15).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .put(2).putInt(5).putInt(2).putInt(0).putShort(512).array()
        ble.emit(Message(MessageType.ARTIFACT_REQUEST, read))
        runCurrent()

        assertEquals(listOf(0x81, 0x82), ble.sentArtifactResponses.map { it[0].toInt() and 0xFF })
        assertEquals(OtaTransfer(1000, 512), status!!.otaTransfer)
        assertFalse("nothing of it goes to the cloud", uplink.published.any { it.second.contentEquals(open) })

        ble.drop(); runCurrent(); job.cancel()
        assertNull(status!!.otaTransfer)
    }

    @Test
    fun `forwards telemetry to the ingest topic`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(1, 2, 3)))
        runCurrent()

        assertTrue(uplink.publishedTo("ingest-cbor", byteArrayOf(1, 2, 3)))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `follows the protocol order - reads first, TX notifications last`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        assertEquals(listOf("prepare", "capabilities", "deviceId", "metadata", "notifications"), ble.operations)
        assertTrue("session metadata is published", uplink.publishedTo("ingest-cbor", "meta".toByteArray()))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `session metadata is published again ahead of everything else after an MQTT reconnect`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        uplink.loseConnection()
        runCurrent()
        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(7)))
        runCurrent()

        assertEquals(2, uplink.connectCount)
        val ingest = uplink.published.filter { it.first == "ingest-cbor" }.map { it.second.toList() }
        val meta = "meta".toByteArray().toList()
        assertEquals("the cloud keeps it per connection", listOf(meta, meta, listOf<Byte>(7)), ingest)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `a reconnect does not re-send old session metadata ahead of the device's newer one`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        val first = FakeBleConnection().apply { sessionMetadata = byteArrayOf(1) }
        val firstJob = launch { runCatching { session(first, uplink, links).run() } }
        runCurrent()
        first.drop(); runCurrent(); firstJob.cancel()

        // The device reboots while the phone is offline: its new session's metadata waits in the buffer.
        uplink.failConnect = true
        uplink.loseConnection()
        val second = FakeBleConnection().apply { sessionMetadata = byteArrayOf(2) }
        val secondJob = launch { runCatching { session(second, uplink, links).run() } }
        runCurrent()
        second.emit(Message(MessageType.TELEMETRY, byteArrayOf(9)))
        uplink.failConnect = false
        advanceTimeBy(60_000); runCurrent()

        val ingest = uplink.published.filter { it.first == "ingest-cbor" }.map { it.second.toList() }
        assertEquals(listOf(listOf<Byte>(1), listOf<Byte>(2), listOf<Byte>(9)), ingest)
        second.drop(); runCurrent(); secondJob.cancel()
    }

    @Test
    fun `routes reported configuration to its topic`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        ble.emit(Message(MessageType.REPORTED_CONFIGURATION, byteArrayOf(5)))
        runCurrent()

        assertTrue(uplink.publishedTo("config-cbor-d2c", byteArrayOf(5)))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `buffers while offline and flushes on recovery`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink().apply { failConnect = true }
        var last: GatewayDeviceState? = null
        val job = launch { runCatching { session(ble, uplink) { last = it }.run() } }
        runCurrent()

        ble.emit(Message(MessageType.TELEMETRY, ByteArray(10)))
        advanceTimeBy(5_000); runCurrent()

        assertTrue("nothing should publish while offline", uplink.published.isEmpty())
        assertTrue("data should be buffered in RAM", (last?.ramBytes ?: 0) > 0)
        assertTrue("and counted as waiting", (last?.pendingMessages ?: 0) > 0)

        uplink.failConnect = false
        advanceTimeBy(30_000); runCurrent()

        assertTrue("buffer should flush once online", uplink.published.isNotEmpty())
        assertEquals(0L, last?.ramBytes)
        assertEquals(0L, last?.diskBytes)
        assertEquals(0L, last?.pendingMessages)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `keeps uploading buffered data after the device disconnects`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink().apply { failConnect = true }
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(7)))
        runCurrent()
        ble.drop()
        runCurrent()
        assertTrue("session ends with the BLE link", job.isCompleted)
        assertFalse(uplink.publishedTo("ingest-cbor", byteArrayOf(7)))

        uplink.failConnect = false // network returns; the device does not
        advanceTimeBy(60_000); runCurrent()

        assertTrue("lingering link uploads it", uplink.publishedTo("ingest-cbor", byteArrayOf(7)))
    }

    @Test
    fun `a quick reconnect takes over the live cloud link`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)

        val first = FakeBleConnection()
        val job1 = launch { session(first, uplink, links).run() }
        runCurrent()
        first.drop(); runCurrent()
        assertTrue(job1.isCompleted)

        val second = FakeBleConnection()
        var status: GatewayDeviceState? = null
        val job2 = launch { session(second, uplink, links) { status = it }.run() }
        runCurrent()
        second.emit(Message(MessageType.TELEMETRY, byteArrayOf(8)))
        runCurrent()

        assertTrue(uplink.publishedTo("ingest-cbor", byteArrayOf(8)))
        assertEquals("MQTT connection is reused, not re-established", 1, uplink.connectCount)
        assertEquals("the new session reports the reused link as connected", true, status?.cloudConnected)
        second.drop(); runCurrent(); job2.cancel()
    }

    @Test
    fun `a device connecting while its recovered buffer uploads reports connected`() = runTest {
        leaveOnFlash("test-device", byteArrayOf(1))
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        links.recover("test-device")
        runCurrent() // recovered link connects and drains; now lingering, still connected

        var status: GatewayDeviceState? = null
        val job = launch { session(FakeBleConnection(), uplink, links) { status = it }.run() }
        runCurrent()

        assertEquals(1, uplink.connectCount)
        assertEquals(true, status?.cloudConnected)
        job.cancel()
    }

    @Test
    fun `buffered data survives a device reconnect while offline`() = runTest {
        val uplink = FakeUplink().apply { failConnect = true }
        val links = registry(backgroundScope, uplink)

        val first = FakeBleConnection()
        val job1 = launch { runCatching { session(first, uplink, links).run() } }
        runCurrent()
        repeat(3) { first.emit(Message(MessageType.TELEMETRY, byteArrayOf(it.toByte(), 1, 2, 3))) }
        advanceTimeBy(2_000); runCurrent()
        first.drop(); runCurrent()
        assertTrue(job1.isCompleted)

        // The device is slow to come back: the new session is still connecting (not yet at the claim).
        val second = FakeBleConnection().apply { prepareGate = CompletableDeferred() }
        var status: GatewayDeviceState? = null
        val job2 = launch { runCatching { session(second, uplink, links) { status = it }.run() } }
        advanceTimeBy(2_000); runCurrent()
        assertTrue("buffer still shown while reconnecting", (status?.ramBytes ?: 0) > 0)

        second.prepareGate!!.complete(Unit)
        advanceTimeBy(2_000); runCurrent()
        assertTrue("buffer still shown after reconnect", (status?.ramBytes ?: 0) + (status?.diskBytes ?: 0) > 0)

        uplink.failConnect = false
        advanceTimeBy(60_000); runCurrent()
        repeat(3) {
            assertTrue("message $it delivered", uplink.publishedTo("ingest-cbor", byteArrayOf(it.toByte(), 1, 2, 3)))
        }
        assertEquals("forwarded counts across the reconnect", uplink.published.size.toLong(), status?.forwarded)
        second.drop(); runCurrent(); job2.cancel()
    }

    @Test
    fun `stops with MqttAuthException on a bad key`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink().apply { authFail = true }
        val error = runCatching { session(ble, uplink).run() }.exceptionOrNull()
        assertTrue("expected MqttAuthException but was $error", error is MqttAuthException)
    }

    @Test
    fun `run returns when the link drops`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { session(ble, uplink).run() }
        runCurrent()

        ble.drop()
        advanceUntilIdle()

        assertTrue(job.isCompleted)
        assertFalse(job.isCancelled)
    }

    @Test
    fun `device filter rejection publishes nothing`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val error = runCatching {
            session(ble, uplink, filter = { _, id -> id == "someone-else" }).run()
        }.exceptionOrNull()

        assertTrue("expected DeviceRejectedException but was $error", error is DeviceRejectedException)
        assertEquals(0, uplink.connectCount)
        assertTrue(uplink.published.isEmpty())
    }

    @Test
    fun `blank device id is rejected`() = runTest {
        val ble = FakeBleConnection(deviceId = "  ")
        val error = runCatching { session(ble, FakeUplink()).run() }.exceptionOrNull()
        assertTrue("expected DeviceRejectedException but was $error", error is DeviceRejectedException)
    }

    @Test
    fun `a second live session with the same device id is refused`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        val first = FakeBleConnection()
        val job = launch { session(first, uplink, links).run() }
        runCurrent()

        val impostor = FakeBleConnection(deviceAddress = "11:22:33:44:55:66")
        val error = runCatching { session(impostor, uplink, links).run() }.exceptionOrNull()

        assertTrue("expected DuplicateDeviceException but was $error", error is DuplicateDeviceException)
        first.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `desired configuration is written in order and acknowledged after the write`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        var status: GatewayDeviceState? = null
        val job = launch { runCatching { session(ble, uplink) { status = it }.run() } }
        runCurrent()

        val acks = mutableListOf<Int>()
        uplink.deliverDesired(byteArrayOf(1)) { acks += 1 }
        uplink.deliverDesired(byteArrayOf(2)) { acks += 2 }
        runCurrent()

        assertEquals(listOf(1, 2), ble.sentDesiredConfig.map { it[0].toInt() })
        assertEquals(listOf(1, 2), acks)
        assertEquals("both counted as received", 2L, status?.received)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `desired configuration waits until the device has reported its configuration`() = runTest {
        val ble = FakeBleConnection().apply { reportsConfiguration = false }
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        var acked = false
        uplink.deliverDesired(byteArrayOf(3)) { acked = true }
        advanceTimeBy(10_000); runCurrent()
        assertTrue("not written before the device can accept it", ble.sentDesiredConfig.isEmpty())
        assertFalse("not acknowledged, so it isn't lost", acked)

        ble.reportConfiguration()
        runCurrent()
        assertTrue(ble.sentDesiredConfig.any { it.contentEquals(byteArrayOf(3)) })
        assertTrue(acked)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `desired configuration queued while the device was away is delivered after it reports`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        val first = FakeBleConnection()
        val job1 = launch { runCatching { session(first, uplink, links).run() } }
        runCurrent()
        first.drop(); runCurrent()

        var acked = false
        uplink.deliverDesired(byteArrayOf(5)) { acked = true } // arrives while the device is disconnected
        runCurrent()

        val second = FakeBleConnection().apply { reportsConfiguration = false }
        val job2 = launch { runCatching { session(second, uplink, links).run() } }
        runCurrent()
        assertTrue("held until the rebooted device reports", second.sentDesiredConfig.isEmpty())
        assertFalse(acked)

        second.reportConfiguration()
        runCurrent()
        assertTrue(second.sentDesiredConfig.any { it.contentEquals(byteArrayOf(5)) })
        assertTrue(acked)
        second.drop(); runCurrent(); job1.cancel(); job2.cancel()
    }

    @Test
    fun `a rebooted device gets the last desired configuration again over the reused cloud link`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        val first = FakeBleConnection()
        val job1 = launch { runCatching { session(first, uplink, links).run() } }
        runCurrent()
        uplink.deliverDesired(byteArrayOf(6)) {}
        runCurrent()
        first.drop(); runCurrent()

        // The broker sends desired configuration only on (re)connect; the link stays connected meanwhile.
        val second = FakeBleConnection().apply { reportsConfiguration = false }
        val job2 = launch { runCatching { session(second, uplink, links).run() } }
        runCurrent()
        assertTrue("held until the rebooted device reports", second.sentDesiredConfig.isEmpty())

        second.reportConfiguration()
        runCurrent()
        assertEquals(listOf(6), second.sentDesiredConfig.map { it[0].toInt() })
        assertEquals("MQTT connection is reused", 1, uplink.connectCount)
        second.drop(); runCurrent(); job1.cancel(); job2.cancel()
    }

    @Test
    fun `newer pending desired configuration replaces the replay`() = runTest {
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink)
        val first = FakeBleConnection()
        val job1 = launch { runCatching { session(first, uplink, links).run() } }
        runCurrent()
        uplink.deliverDesired(byteArrayOf(6)) {}
        runCurrent()
        first.drop(); runCurrent()
        uplink.deliverDesired(byteArrayOf(7)) {} // changed in the portal while the device was away
        runCurrent()

        val second = FakeBleConnection()
        val job2 = launch { runCatching { session(second, uplink, links).run() } }
        runCurrent()
        assertEquals(listOf(7), second.sentDesiredConfig.map { it[0].toInt() })
        second.drop(); runCurrent(); job1.cancel(); job2.cancel()
    }

    @Test
    fun `failed desired configuration write is not acknowledged and is retried`() = runTest {
        val ble = FakeBleConnection().apply { failDesiredWrites = 1 }
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        var acked = false
        uplink.deliverDesired(byteArrayOf(9)) { acked = true }
        runCurrent()
        assertFalse("not acknowledged while the write failed", acked)

        advanceTimeBy(3_000); runCurrent()
        assertTrue(ble.sentDesiredConfig.any { it.contentEquals(byteArrayOf(9)) })
        assertTrue(acked)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `poison message is dropped while connected`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink().apply { poison = { it[0].toInt() == 66 } }
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(66)))
        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(1)))
        advanceTimeBy(120_000); runCurrent()

        assertTrue("later messages are not blocked", uplink.publishedTo("ingest-cbor", byteArrayOf(1)))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `poison message that makes the broker disconnect is eventually dropped`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink().apply {
            poison = { it[0].toInt() == 66 }
            disconnectOnPoison = true
        }
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(66)))
        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(1)))
        advanceTimeBy(600_000); runCurrent()

        assertTrue("later messages are not blocked", uplink.publishedTo("ingest-cbor", byteArrayOf(1)))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `recover drains a buffer left on disk by an earlier run`() = runTest {
        leaveOnFlash("orphan", byteArrayOf(4, 2))
        assertEquals(listOf("orphan"), store.storedDeviceIds())

        val uplink = FakeUplink()
        registry(backgroundScope, uplink).recover("orphan")
        advanceTimeBy(60_000); runCurrent()

        assertTrue(uplink.publishedTo("ingest-cbor", byteArrayOf(4, 2)))
        assertTrue("nothing left on flash", store.storedDeviceIds().isEmpty())
    }

    @Test
    fun `a connection lost to the network never costs a message`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        uplink.networkDrops = 25 // far more often than any poison limit allows
        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(42)))
        advanceTimeBy(30 * 60_000L); runCurrent()

        assertEquals("every drop was hit", 0, uplink.networkDrops)
        assertTrue("delivered once the network holds", uplink.publishedTo("ingest-cbor", byteArrayOf(42)))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `a broker over its quota never costs a message`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        uplink.throttled = 25 // far more refusals than a bad message is allowed
        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(42)))
        advanceTimeBy(30 * 60_000L); runCurrent()

        assertEquals(0, uplink.throttled)
        assertTrue("delivered once the broker takes it", uplink.publishedTo("ingest-cbor", byteArrayOf(42)))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `a slow uplink working through a window is not mistaken for a stalled one`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        uplink.holdAcks = true
        repeat(5) { ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(it.toByte()))) }
        runCurrent()
        // One acknowledgement every 15 s: the last one arrives after a minute, each within the timeout.
        repeat(5) {
            advanceTimeBy(15_000); runCurrent()
            uplink.acknowledgeOne()
            runCurrent()
        }

        assertEquals("never forced to reconnect", 1, uplink.connectCount)
        assertEquals("nothing sent twice", 5, uplink.published.count { it.first == "ingest-cbor" && it.second.size == 1 })
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `an uplink that stops acknowledging altogether is reconnected`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        uplink.holdAcks = true
        repeat(3) { ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(it.toByte()))) }
        advanceTimeBy(20_001); runCurrent() // nothing acknowledged for the whole timeout
        uplink.holdAcks = false
        advanceTimeBy(5_000); runCurrent()

        assertEquals("the stalled connection was replaced", 2, uplink.connectCount)
        repeat(3) { assertTrue(uplink.published.count { p -> p.second.contentEquals(byteArrayOf(it.toByte())) } >= 2) }
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `desired configuration the broker delivers again is written once and acknowledged for each delivery`() = runTest {
        val ble = FakeBleConnection().apply { reportsConfiguration = false }
        val uplink = FakeUplink()
        var status: GatewayDeviceState? = null
        val job = launch { runCatching { session(ble, uplink) { status = it }.run() } }
        runCurrent()

        var acks = 0
        repeat(3) { uplink.deliverDesired(byteArrayOf(8)) { acks++ } } // redelivered on two reconnects
        runCurrent()
        assertEquals("one configuration, however often it was delivered", 1L, status?.received)

        ble.reportConfiguration()
        runCurrent()
        assertEquals(1, ble.sentDesiredConfig.size)
        assertEquals(3, acks)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `several messages are in flight at once and keep their order`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent() // session metadata and the reported configuration are through

        uplink.holdAcks = true
        repeat(5) { ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(it.toByte()))) }
        runCurrent()
        assertEquals("all sent before any is acknowledged", 5, uplink.inFlight)

        uplink.acknowledgeAll()
        runCurrent()
        val telemetry = uplink.published.filter { it.first == "ingest-cbor" && it.second.size == 1 }
        assertEquals(listOf(0, 1, 2, 3, 4), telemetry.map { it.second[0].toInt() })
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `a poison message among several in flight does not take the others with it`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink().apply {
            poison = { it[0].toInt() == 66 }
            disconnectOnPoison = true // the broker hangs up, failing everything that was in flight
        }
        var status: GatewayDeviceState? = null
        val job = launch { runCatching { session(ble, uplink) { status = it }.run() } }
        runCurrent()

        listOf(1, 66, 2, 3).forEach { ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(it.toByte()))) }
        advanceTimeBy(600_000); runCurrent()

        listOf(1, 2, 3).forEach {
            assertTrue("message $it delivered", uplink.publishedTo("ingest-cbor", byteArrayOf(it.toByte())))
        }
        assertFalse(uplink.publishedTo("ingest-cbor", byteArrayOf(66)))
        assertEquals("nothing left waiting", 0L, status?.pendingMessages)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `a lost connection is reported at once and re-established without waiting for data`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        var status: GatewayDeviceState? = null
        val job = launch { runCatching { session(ble, uplink) { status = it }.run() } }
        runCurrent()
        assertEquals(true, status?.cloudConnected)

        uplink.loseConnection()
        assertEquals("reported as it happens", false, status?.cloudConnected)

        runCurrent() // no time passes: the idle drain loop is woken rather than polling
        assertEquals(2, uplink.connectCount)
        assertEquals(true, status?.cloudConnected)
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `without a network it waits instead of retrying, and connects the moment one is back`() = runTest {
        val online = MutableStateFlow(false)
        val uplink = FakeUplink()
        val links = registry(backgroundScope, uplink, online)
        val ble = FakeBleConnection()
        var status: GatewayDeviceState? = null
        val job = launch { runCatching { session(ble, uplink, links) { status = it }.run() } }
        advanceTimeBy(5 * 60_000L); runCurrent()

        assertEquals("no attempts while there is no network", 0, uplink.connectAttempts)
        assertNull("being offline is not an error", status?.error)
        assertTrue("still buffering", (status?.pendingMessages ?: 0) > 0)

        online.value = true
        runCurrent() // no time passes
        assertEquals(1, uplink.connectCount)
        assertTrue(uplink.publishedTo("ingest-cbor", "meta".toByteArray()))
        ble.drop(); runCurrent(); job.cancel()
    }

    @Test
    fun `a failed connect is retried as soon as the network returns`() = runTest {
        val online = MutableStateFlow(true)
        val uplink = FakeUplink().apply { failConnect = true }
        val links = registry(backgroundScope, uplink, online)
        var status: GatewayDeviceState? = null
        val job = launch { runCatching { session(FakeBleConnection(), uplink, links) { status = it }.run() } }
        advanceTimeBy(120_000); runCurrent() // by now the backoff between attempts is at its 30 s maximum
        assertEquals(DeviceError.Kind.CLOUD_UNREACHABLE, status?.error?.kind)
        val attempts = uplink.connectAttempts

        online.value = false
        runCurrent()
        uplink.failConnect = false
        online.value = true
        runCurrent() // no time passes

        assertEquals(attempts + 1, uplink.connectAttempts)
        assertEquals(true, status?.cloudConnected)
        assertNull("a working connection clears the cloud error", status?.error)
        job.cancel()
    }

    @Test
    fun `a rejected key is reported as such and the session ends disconnected`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink().apply { authFail = true }
        var status: GatewayDeviceState? = null
        runCatching { session(ble, uplink) { status = it }.run() }

        assertEquals(DeviceError.Kind.AUTH_REJECTED, status?.error?.kind)
        assertEquals("the BLE link went down with the session", ConnectionState.DISCONNECTED, status?.ble)
    }

    @Test
    fun `two sessions claiming one device id during a hand-over cannot both win`() = runTest {
        leaveOnFlash("test-device", byteArrayOf(1))
        // The lingering upload takes its time to stop, which is when a second claim used to slip through.
        val gate = CompletableDeferred<Unit>()
        val uplink = FakeUplink().apply { connectGate = gate }
        val links = registry(backgroundScope, uplink)
        links.recover("test-device")
        runCurrent()

        val first = async { runCatching { links.claim("test-device") {} } }
        try {
            runCurrent() // waiting for the lingering upload to let go
            assertFalse(first.isCompleted)
            val second = runCatching { links.claim("test-device") {} }
            assertTrue("expected DuplicateDeviceException but was $second", second.exceptionOrNull() is DuplicateDeviceException)
        } finally {
            gate.complete(Unit) // also when an assertion failed: nothing may be left waiting on it
        }
        runCurrent()
        assertTrue(first.await().isSuccess)
    }

    @Test
    fun `the cloud link is handed over only once the session's own upload loop has let go`() = runTest {
        // The session's loop is in the middle of something it can't drop at once.
        val gate = CompletableDeferred<Unit>()
        val uplink = FakeUplink().apply { connectGate = gate }
        val ble = FakeBleConnection()
        val job = launch { runCatching { session(ble, uplink).run() } }
        try {
            runCurrent()

            ble.drop()
            runCurrent()
            assertFalse("the session waits for its loop instead of leaving it running", job.isCompleted)
            assertEquals("no second loop has started on the link meanwhile", 1, uplink.connectAttempts)
        } finally {
            gate.complete(Unit) // also when an assertion failed: nothing may be left waiting on it
        }
        runCurrent()
        assertTrue(job.isCompleted)
        assertEquals("a link has one consumer at a time", 1, uplink.maxConcurrentConnects)
    }

    @Test
    fun `signal strength is read only while someone is watching`() = runTest {
        val ble = FakeBleConnection()
        val watching = MutableStateFlow(false)
        val job = launch { runCatching { session(ble, FakeUplink(), rssiWanted = watching).run() } }
        advanceTimeBy(60_000); runCurrent()
        assertEquals(0, ble.rssiReads)

        watching.value = true
        advanceTimeBy(12_000); runCurrent()
        assertTrue("expected a few reads but was ${ble.rssiReads}", ble.rssiReads in 2..4)
        ble.drop(); runCurrent(); job.cancel()
    }
}

/** A controllable [BleConnection] for tests. */
internal class FakeBleConnection(
    override val deviceAddress: String = "AA:BB:CC:DD:EE:FF",
    private val deviceId: String = "test-device",
) : BleConnection {
    private val stateFlow = MutableStateFlow(ConnectionState.DISCONNECTED)
    override val state: StateFlow<ConnectionState> = stateFlow
    private val channel = Channel<Message>(Channel.UNLIMITED)
    override val incoming: Flow<Message> = channel.receiveAsFlow()
    override val mtu = 247
    val sentDesiredConfig = mutableListOf<ByteArray>()
    var failDesiredWrites = 0

    val operations = mutableListOf<String>()
    var prepareGate: CompletableDeferred<Unit>? = null
    /** Makes [prepare] fail, as a connection attempt that doesn't get through would. */
    var prepareFailure: Throwable? = null
    var closed = false
        private set

    override suspend fun prepare() {
        stateFlow.value = ConnectionState.CONNECTING
        prepareGate?.await()
        prepareFailure?.let { throw it }
        operations += "prepare"
        stateFlow.value = ConnectionState.PREPARING
    }
    var capabilities = DeviceCapabilities(1, 1, 0)
    override suspend fun readCapabilities() = capabilities.also { operations += "capabilities" }
    /** Like the Device SDK: once notifications are on, the device sends its reported configuration. */
    var reportsConfiguration = true
    override suspend fun startStreaming() {
        operations += "notifications"
        stateFlow.value = ConnectionState.READY
        if (reportsConfiguration) reportConfiguration()
    }
    fun reportConfiguration() { emit(Message(MessageType.REPORTED_CONFIGURATION, byteArrayOf(0x7E))) }
    override suspend fun readDeviceId(): String { operations += "deviceId"; return deviceId }
    var sessionMetadata = "meta".toByteArray()
    override suspend fun readSessionMetadata(): ByteArray { operations += "metadata"; return sessionMetadata }
    var rssiReads = 0
    override suspend fun readRssi(): Int { rssiReads++; return -55 }
    override suspend fun sendDesiredConfiguration(payload: ByteArray) {
        if (failDesiredWrites > 0) {
            failDesiredWrites--
            throw IllegalStateException("write failed")
        }
        sentDesiredConfig += payload
    }

    /** OTA commands the device has acknowledged, in order. */
    val sentOtaCommands = mutableListOf<ByteArray>()

    /** While set, an OTA command waits for the device's ACK until this completes (or throws with it). */
    var otaAck: CompletableDeferred<Unit>? = null
    override suspend fun sendOtaCommand(payload: ByteArray) {
        otaAck?.await()
        sentOtaCommands += payload
    }

    val sentArtifactResponses = mutableListOf<ByteArray>()
    override suspend fun sendArtifactResponse(payload: ByteArray) { sentArtifactResponses += payload }
    override suspend fun close() { closed = true }

    fun emit(message: Message) { channel.trySend(message) }
    fun drop() { stateFlow.value = ConnectionState.DISCONNECTED }
}

/**
 * A controllable [Uplink] for tests. It follows the contract of the real one — in particular, a publish
 * cut off by a lost connection throws [UplinkDisconnectedException] — which `MqttUplinkBrokerTest` holds
 * the real client to.
 */
internal class FakeUplink : Uplink {
    @Volatile var connected = false
    var failConnect = false
    var authFail = false
    var connectCount = 0
    var connectAttempts = 0
    var poison: (ByteArray) -> Boolean = { false }
    var disconnectOnPoison = false

    /** The next this-many publishes lose the connection to the network (nothing to do with the message). */
    var networkDrops = 0

    /** The next this-many publishes are refused because the broker is over its quota. */
    var throttled = 0

    /** While set, publishes stay in flight until [acknowledgeAll]. */
    var holdAcks = false
    private val unacknowledged = ArrayList<CompletableDeferred<Unit>>()
    val inFlight: Int get() = unacknowledged.size
    var maxInFlight = 0

    val published = mutableListOf<Pair<String, ByteArray>>()

    override val isConnected: Boolean get() = connected
    override var desiredConfigurationHandler: ((ByteArray, () -> Unit) -> Unit)? = null
    override var otaCommandHandler: ((ByteArray, () -> Unit) -> Unit)? = null
    override var onConnectionLost: (() -> Unit)? = null

    override var isOtaSubscribed = false
    override var mayHoldOtaSubscription = false

    /** "subscribe" / "unsubscribe", each with how many messages had been published before it. */
    val otaSubscriptionCalls = mutableListOf<Pair<String, Int>>()
    var failOtaSubscribe = 0

    override suspend fun subscribeOta() {
        if (!connected) throw UplinkDisconnectedException("not connected", rejectedByBroker = false)
        mayHoldOtaSubscription = true
        if (failOtaSubscribe > 0) {
            failOtaSubscribe--
            throw RuntimeException("refused")
        }
        otaSubscriptionCalls += "subscribe" to published.size
        isOtaSubscribed = true
    }

    override suspend fun unsubscribeOta() {
        if (!connected) throw UplinkDisconnectedException("not connected", rejectedByBroker = false)
        otaSubscriptionCalls += "unsubscribe" to published.size
        isOtaSubscribed = false
        mayHoldOtaSubscription = false
    }

    /** While set, connect() does not return — not even when cancelled — until it completes. */
    var connectGate: CompletableDeferred<Unit>? = null

    private var connecting = 0
    var maxConcurrentConnects = 0

    override suspend fun connect() {
        connectAttempts++
        connecting++
        maxConcurrentConnects = maxOf(maxConcurrentConnects, connecting)
        try {
            connectGate?.let { gate -> withContext(NonCancellable) { gate.await() } }
            if (authFail) throw MqttAuthException("bad key")
            if (failConnect) throw RuntimeException("offline")
            connectCount++
            connected = true
            isOtaSubscribed = false
        } finally {
            connecting--
        }
    }

    override suspend fun publish(topic: String, payload: ByteArray) {
        if (!connected) throw UplinkDisconnectedException("not connected", rejectedByBroker = false)
        if (throttled > 0) {
            throttled--
            throw UplinkThrottledException("quota exceeded")
        }
        if (networkDrops > 0) {
            networkDrops--
            connected = false
            throw UplinkDisconnectedException("connection lost", rejectedByBroker = false)
        }
        if (poison(payload)) {
            if (disconnectOnPoison) {
                connected = false
                throw UplinkDisconnectedException("broker hung up", rejectedByBroker = true)
            }
            throw RuntimeException("rejected")
        }
        published += topic to payload // sent, in this order
        if (holdAcks) {
            val ack = CompletableDeferred<Unit>()
            unacknowledged += ack
            maxInFlight = maxOf(maxInFlight, unacknowledged.size)
            ack.await()
        }
    }

    override suspend fun disconnect() { connected = false }

    fun acknowledgeAll() {
        val acks = unacknowledged.toList()
        unacknowledged.clear()
        acks.forEach { it.complete(Unit) }
    }

    /** Acknowledges the oldest publish in flight. */
    fun acknowledgeOne() {
        unacknowledged.removeFirstOrNull()?.complete(Unit)
    }

    /** The connection drops while nothing is being published. */
    fun loseConnection() {
        connected = false
        onConnectionLost?.invoke()
    }

    fun publishedTo(topic: String, payload: ByteArray) =
        published.any { it.first == topic && it.second.contentEquals(payload) }

    fun deliverDesired(payload: ByteArray, ack: () -> Unit) {
        desiredConfigurationHandler?.invoke(payload, ack)
    }
}

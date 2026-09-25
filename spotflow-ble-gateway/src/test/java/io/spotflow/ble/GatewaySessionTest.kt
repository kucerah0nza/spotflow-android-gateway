package io.spotflow.ble

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import io.spotflow.ble.cloud.MqttAuthException
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.PersistentMessageQueue
import io.spotflow.ble.cloud.StoreAndForwardBuffer
import io.spotflow.ble.cloud.Uplink
import io.spotflow.ble.protocol.Message
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
class GatewaySessionTest {

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val config = MqttConfig()

    @Before
    fun setUp() {
        context.databaseList().forEach { context.deleteDatabase(it) }
    }

    private fun registry(scope: CoroutineScope, uplink: FakeUplink, diskMaxBytes: Long = 10_000) =
        CloudLinkRegistry(scope) { deviceId, push ->
            CloudLink(
                deviceId,
                StoreAndForwardBuffer(PersistentMessageQueue(context, deviceId, diskMaxBytes), 1_000),
                uplink,
                push,
            )
        }

    /** Mirrors the gateway: one status entry per address, shared by every session and cloud link. */
    private val board = HashMap<String, GatewayDeviceState>()

    private fun TestScope.session(
        ble: FakeBleConnection,
        uplink: FakeUplink,
        links: CloudLinkRegistry = registry(backgroundScope, uplink),
        filter: DeviceFilter? = null,
        onStatus: (GatewayDeviceState) -> Unit = {},
    ): GatewaySession {
        val address = ble.deviceAddress
        val push: StatusPush = { change ->
            val state = synchronized(board) {
                change(board[address] ?: GatewayDeviceState(address)).also { board[address] = it }
            }
            onStatus(state)
        }
        return GatewaySession(ble, config, links, push, filter)
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

        uplink.failConnect = false
        advanceTimeBy(30_000); runCurrent()

        assertTrue("buffer should flush once online", uplink.published.isNotEmpty())
        assertEquals(0L, last?.ramBytes)
        assertEquals(0L, last?.diskBytes)
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
        PersistentMessageQueue(context, "test-device", 10_000).apply {
            enqueue(1, "ingest-cbor", byteArrayOf(1))
            close()
        }
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
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        val acks = mutableListOf<Int>()
        uplink.deliverDesired(byteArrayOf(1)) { acks += 1 }
        uplink.deliverDesired(byteArrayOf(2)) { acks += 2 }
        runCurrent()

        assertEquals(listOf(1, 2), ble.sentDesiredConfig.map { it[0].toInt() })
        assertEquals(listOf(1, 2), acks)
        ble.drop(); runCurrent(); job.cancel()
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
        PersistentMessageQueue(context, "orphan", 10_000).apply {
            enqueue(1, "ingest-cbor", byteArrayOf(4, 2))
            close()
        }
        assertEquals(listOf("orphan"), PersistentMessageQueue.storedDeviceIds(context))

        val uplink = FakeUplink()
        registry(backgroundScope, uplink).recover("orphan")
        advanceTimeBy(60_000); runCurrent()

        assertTrue(uplink.publishedTo("ingest-cbor", byteArrayOf(4, 2)))
        assertTrue("drained buffer file is deleted", PersistentMessageQueue.storedDeviceIds(context).isEmpty())
    }
}

/** A controllable [BleConnection] for tests. */
private class FakeBleConnection(
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
    override suspend fun prepare() {
        stateFlow.value = ConnectionState.CONNECTING
        prepareGate?.await()
        operations += "prepare"
        stateFlow.value = ConnectionState.PREPARING
    }
    override suspend fun readProtocolVersion(): Int { operations += "capabilities"; return 1 }
    override suspend fun startStreaming() { operations += "notifications"; stateFlow.value = ConnectionState.READY }
    override suspend fun readDeviceId(): String { operations += "deviceId"; return deviceId }
    override suspend fun readSessionMetadata(): ByteArray { operations += "metadata"; return "meta".toByteArray() }
    override suspend fun readRssi() = -55
    override suspend fun sendDesiredConfiguration(payload: ByteArray) {
        if (failDesiredWrites > 0) {
            failDesiredWrites--
            throw IllegalStateException("write failed")
        }
        sentDesiredConfig += payload
    }
    override suspend fun close() {}

    fun emit(message: Message) { channel.trySend(message) }
    fun drop() { stateFlow.value = ConnectionState.DISCONNECTED }
}

/** A controllable [Uplink] for tests. */
private class FakeUplink : Uplink {
    @Volatile var connected = false
    var failConnect = false
    var authFail = false
    var connectCount = 0
    var poison: (ByteArray) -> Boolean = { false }
    var disconnectOnPoison = false
    val published = mutableListOf<Pair<String, ByteArray>>()

    override val isConnected: Boolean get() = connected
    override var desiredConfigurationHandler: ((ByteArray, () -> Unit) -> Unit)? = null

    override suspend fun connect() {
        if (authFail) throw MqttAuthException("bad key")
        if (failConnect) throw RuntimeException("offline")
        connectCount++
        connected = true
    }

    override suspend fun publish(topic: String, payload: ByteArray) {
        if (!connected) throw RuntimeException("not connected")
        if (poison(payload)) {
            if (disconnectOnPoison) connected = false
            throw RuntimeException("rejected")
        }
        published += topic to payload
    }

    override suspend fun disconnect() { connected = false }

    fun publishedTo(topic: String, payload: ByteArray) =
        published.any { it.first == topic && it.second.contentEquals(payload) }

    fun deliverDesired(payload: ByteArray, ack: () -> Unit) {
        desiredConfigurationHandler?.invoke(payload, ack)
    }
}

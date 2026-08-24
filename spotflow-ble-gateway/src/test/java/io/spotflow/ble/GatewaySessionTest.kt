package io.spotflow.ble

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import io.spotflow.ble.cloud.MqttAuthException
import io.spotflow.ble.cloud.MqttConfig
import io.spotflow.ble.cloud.StaticIngestKey
import io.spotflow.ble.cloud.Uplink
import io.spotflow.ble.protocol.Message
import io.spotflow.ble.protocol.MessageType
import io.spotflow.ble.transport.BleConnection
import io.spotflow.ble.transport.ConnectionState
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
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

@RunWith(RobolectricTestRunner::class)
class GatewaySessionTest {

    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Before
    fun setUp() {
        context.deleteDatabase("spotflow_buffer_test-device.db")
    }

    private fun session(
        ble: FakeBleConnection,
        uplink: FakeUplink,
        onStatus: (GatewayDeviceState) -> Unit = {},
    ) = GatewaySession(
        context = context,
        connection = ble,
        credentials = StaticIngestKey("key"),
        mqttConfig = MqttConfig(),
        onStatus = onStatus,
        uplinkFactory = { uplink },
    )

    @Test
    fun `forwards telemetry to the ingest topic`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        ble.emit(Message(MessageType.TELEMETRY, byteArrayOf(1, 2, 3)))
        runCurrent()

        assertTrue(
            uplink.published.any { it.first == "ingest-cbor" && it.second.contentEquals(byteArrayOf(1, 2, 3)) },
        )
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

        assertTrue(uplink.published.any { it.first == "config-cbor-d2c" && it.second.contentEquals(byteArrayOf(5)) })
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
    fun `desired configuration is written to the device`() = runTest {
        val ble = FakeBleConnection()
        val uplink = FakeUplink()
        val job = launch { runCatching { session(ble, uplink).run() } }
        runCurrent()

        uplink.desiredConfigurationHandler?.invoke(byteArrayOf(9, 9))
        runCurrent()

        assertTrue(ble.sentDesiredConfig.any { it.contentEquals(byteArrayOf(9, 9)) })
        ble.drop(); runCurrent(); job.cancel()
    }
}

/** A controllable [BleConnection] for tests. */
private class FakeBleConnection : BleConnection {
    override val deviceAddress = "AA:BB:CC:DD:EE:FF"
    private val stateFlow = MutableStateFlow(ConnectionState.DISCONNECTED)
    override val state: StateFlow<ConnectionState> = stateFlow
    private val channel = Channel<Message>(Channel.UNLIMITED)
    override val incoming: Flow<Message> = channel.receiveAsFlow()
    override val mtu = 247
    val sentDesiredConfig = mutableListOf<ByteArray>()

    override suspend fun prepare() { stateFlow.value = ConnectionState.READY }
    override suspend fun readDeviceId() = "test-device"
    override suspend fun readSessionMetadata() = "meta".toByteArray()
    override suspend fun readRssi() = -55
    override suspend fun sendDesiredConfiguration(payload: ByteArray) { sentDesiredConfig += payload }
    override suspend fun close() {}

    fun emit(message: Message) { channel.trySend(message) }
    fun drop() { stateFlow.value = ConnectionState.DISCONNECTED }
}

/** A controllable [Uplink] for tests. */
private class FakeUplink : Uplink {
    @Volatile var connected = false
    var failConnect = false
    var authFail = false
    val published = mutableListOf<Pair<String, ByteArray>>()

    override val isConnected: Boolean get() = connected
    override var desiredConfigurationHandler: ((ByteArray) -> Unit)? = null

    override suspend fun connect() {
        if (authFail) throw MqttAuthException("bad key")
        if (failConnect) throw RuntimeException("offline")
        connected = true
    }

    override suspend fun publish(topic: String, payload: ByteArray) {
        if (!connected) throw RuntimeException("not connected")
        published += topic to payload
    }

    override suspend fun disconnect() { connected = false }
}

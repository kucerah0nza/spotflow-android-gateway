package io.spotflow.ble.transport

import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothProfile
import io.mockk.verify
import io.spotflow.ble.protocol.FrameCodec
import io.spotflow.ble.protocol.GattProfile
import io.spotflow.ble.protocol.Message
import io.spotflow.ble.protocol.MessageType
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.io.IOException

/** The GATT engine against a [FakePeripheral]: which callback completes which operation, and when it gives up. */
@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [26, 35]) // both GATT dialects: values through the characteristic (before Android 13), and as arguments
class SpotflowGattSessionTest {

    private val peripheral = FakePeripheral()
    private val session = SpotflowGattSession().apply { attachGatt(peripheral.gatt) }

    /** Delivers every callback the peripheral owes, letting the session react to each. */
    private fun TestScope.answerAll() {
        runCurrent()
        while (peripheral.deliver(session.callback)) runCurrent()
    }

    private fun TestScope.prepared() {
        val preparing = launch { session.prepare() }
        answerAll()
        assertTrue(preparing.isCompleted)
    }

    @Test
    fun `prepare discovers services and negotiates the MTU`() = runTest {
        prepared()

        assertEquals(ConnectionState.PREPARING, session.state.value)
        assertEquals(247, session.mtu)
    }

    @Test
    fun `a read is completed only by the result for the characteristic it asked for`() = runTest {
        prepared()
        val read = async { session.read(GattProfile.DEVICE_ID) }
        runCurrent()

        // The host reads a characteristic of its own without going through runExclusive, and forwards
        // the result as the contract says. It must not be taken for the device ID.
        peripheral.read(session.callback, peripheral.hostCharacteristic, "not-the-device-id".toByteArray())
        runCurrent()
        assertFalse(read.isCompleted)

        answerAll()
        assertEquals("attached-device", read.await().toString(Charsets.UTF_8))
    }

    @Test
    fun `a write result for another characteristic does not complete the gateway's write`() = runTest {
        prepared()
        val write = launch { session.writeMessage(MessageType.DESIRED_CONFIGURATION, byteArrayOf(1, 2, 3)) }
        runCurrent()

        session.callback.onCharacteristicWrite(peripheral.gatt, peripheral.hostCharacteristic, BluetoothGatt.GATT_SUCCESS)
        runCurrent()
        assertFalse(write.isCompleted)

        answerAll()
        assertTrue(write.isCompleted)
        val message = FrameCodec.Reassembler().onFragment(peripheral.rxWrites.single())
        assertEquals(Message(MessageType.DESIRED_CONFIGURATION, byteArrayOf(1, 2, 3)), message)
    }

    @Test
    fun `a descriptor write on another characteristic does not start the stream`() = runTest {
        prepared()
        val starting = launch { session.startStreaming() }
        runCurrent()

        session.callback.onDescriptorWrite(
            peripheral.gatt, peripheral.hostCharacteristic.getDescriptor(GattProfile.CCCD), BluetoothGatt.GATT_SUCCESS,
        )
        runCurrent()
        assertFalse(starting.isCompleted)

        answerAll()
        assertTrue(starting.isCompleted)
        assertEquals(ConnectionState.READY, session.state.value)
    }

    @Test
    fun `an operation the peripheral never answers times out`() = runTest {
        prepared()
        val read = async { runCatching { session.read(GattProfile.DEVICE_ID) } }
        runCurrent()
        peripheral.swallow()

        advanceTimeBy(10_001); runCurrent()

        val error = read.await().exceptionOrNull()
        assertTrue("expected a timeout but was $error", error is IOException)
    }

    @Test
    fun `a disconnect fails the pending operation`() = runTest {
        prepared()
        val read = async { runCatching { session.read(GattProfile.DEVICE_ID) } }
        runCurrent()

        session.callback.onConnectionStateChange(peripheral.gatt, 8, BluetoothProfile.STATE_DISCONNECTED)
        runCurrent()

        assertTrue(read.await().isFailure)
        assertEquals(ConnectionState.DISCONNECTED, session.state.value)
    }

    @Test
    fun `starting the stream turns notifications on, stopping it turns them off`() = runTest {
        prepared()
        launch { session.startStreaming() }
        answerAll()
        assertArrayEquals(BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE, peripheral.cccdWrites.single())

        val stopping = launch { session.stopStreaming() }
        answerAll()

        assertTrue(stopping.isCompleted)
        assertArrayEquals(BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE, peripheral.cccdWrites.last())
        verify { peripheral.gatt.setCharacteristicNotification(peripheral.tx, false) }
    }

    @Test
    fun `a link that drops while the stream is being started is not reported as ready`() = runTest {
        prepared()
        val starting = async { runCatching { session.startStreaming() } }
        runCurrent()

        // The disconnect and the answer to the CCCD write arrive back to back, before the session runs.
        session.callback.onConnectionStateChange(peripheral.gatt, 8, BluetoothProfile.STATE_DISCONNECTED)
        peripheral.deliver(session.callback)
        runCurrent()

        assertTrue(starting.await().isFailure)
        assertEquals("or the session would wait forever for an end it already missed", ConnectionState.DISCONNECTED, session.state.value)
    }

    @Test
    fun `stopping the stream gives up quickly if the peripheral does not answer`() = runTest {
        prepared()
        launch { session.startStreaming() }
        answerAll()

        val stopping = launch { session.stopStreaming() }
        runCurrent()
        peripheral.swallow()
        advanceTimeBy(2_001); runCurrent()

        assertTrue("detaching must not hang on it", stopping.isCompleted)
    }

    @Test
    fun `stopping a stream that never started does nothing`() = runTest {
        prepared()

        session.stopStreaming()

        assertTrue(peripheral.cccdWrites.isEmpty())
    }

    @Test
    fun `TX stream fragments are reassembled, other notifications ignored`() = runTest {
        prepared()
        launch { session.startStreaming() }
        answerAll()
        val received = async { session.incoming.first() }
        runCurrent()

        peripheral.notify(session.callback, peripheral.hostCharacteristic, byteArrayOf(2, 3, 0, 1, 0, 9))
        val payload = ByteArray(60) { it.toByte() }
        FrameCodec.fragment(MessageType.TELEMETRY, payload, seq = 5, mtu = 23).forEach {
            peripheral.notify(session.callback, peripheral.tx, it)
        }
        runCurrent()

        assertEquals(Message(MessageType.TELEMETRY, payload), received.await())
    }

    @Test
    fun `an attached GATT that is already up is used at once`() = runTest {
        session.awaitConnected(isConnected = { true }, timeoutMs = 30_000)
        assertEquals(0L, testScheduler.currentTime)
    }

    @Test
    fun `an attached GATT that is still connecting is waited for`() = runTest {
        val waiting = launch { session.awaitConnected(isConnected = { false }, timeoutMs = 30_000) }
        runCurrent()
        assertFalse(waiting.isCompleted)
        assertEquals(ConnectionState.CONNECTING, session.state.value)

        session.callback.onConnectionStateChange(peripheral.gatt, BluetoothGatt.GATT_SUCCESS, BluetoothProfile.STATE_CONNECTED)
        runCurrent()
        assertTrue(waiting.isCompleted)
    }

    @Test
    fun `an attached GATT that never connects fails the attempt`() = runTest {
        val waiting = async { runCatching { session.awaitConnected(isConnected = { false }, timeoutMs = 30_000) } }
        advanceTimeBy(30_001); runCurrent()

        assertTrue(waiting.await().exceptionOrNull() is IOException)
    }

    @Test
    fun `detaching leaves the host's GATT connected, closing a managed one does not`() = runTest {
        session.detach()
        verify(exactly = 0) { peripheral.gatt.disconnect() }
        verify(exactly = 0) { peripheral.gatt.close() }
        assertEquals(ConnectionState.DISCONNECTED, session.state.value)

        val other = FakePeripheral()
        val managed = SpotflowGattSession().apply { attachGatt(other.gatt) }
        managed.disconnectAndClose()
        verify { other.gatt.disconnect() }
        verify { other.gatt.close() }
    }

    @Test
    fun `closing a session that never got a GATT still ends it cleanly`() = runTest {
        val fresh = SpotflowGattSession()
        val failed = runCatching { fresh.connect { null } } // connectGatt returned null

        assertTrue(failed.isFailure)
        assertEquals("not left showing CONNECTING", ConnectionState.DISCONNECTED, fresh.state.value)
    }

    // ---- NEEDS_ACK messages (OTA commands) -----------------------------------------------------------

    /** Sends an OTA command and answers its writes, leaving the session waiting for the device's verdict. */
    private fun TestScope.sendOta(payload: ByteArray) = async { runCatching { session.writeAcknowledged(MessageType.OTA_C2D, payload) } }
        .also { answerAll() }

    private fun TestScope.control(type: MessageType, ackedType: MessageType, seq: Int) {
        peripheral.notify(session.callback, peripheral.tx, byteArrayOf(type.value.toByte(), ackedType.value.toByte(), seq.toByte()))
        runCurrent()
    }

    @Test
    fun `an acknowledged message is written with NEEDS_ACK and completes on the device's ACK for it`() = runTest {
        prepared()
        val sending = sendOta(byteArrayOf(1, 2, 3))

        val frame = peripheral.rxWrites.single()
        assertEquals(MessageType.OTA_C2D.value, frame[0].toInt())
        assertEquals(FrameCodec.FLAG_NEEDS_ACK, frame[1].toInt() and FrameCodec.FLAG_NEEDS_ACK)
        val seq = frame[2].toInt()

        // Not an answer to it: another type, another sequence number.
        control(MessageType.ACK, MessageType.DESIRED_CONFIGURATION, seq)
        control(MessageType.ACK, MessageType.OTA_C2D, seq + 1)
        assertFalse(sending.isCompleted)

        control(MessageType.ACK, MessageType.OTA_C2D, seq)
        assertTrue(sending.await().isSuccess)
    }

    @Test
    fun `a NACK retransmits the same message, with the same sequence number, after a backoff`() = runTest {
        prepared()
        val sending = sendOta(byteArrayOf(4, 5))
        val first = peripheral.rxWrites.single()

        control(MessageType.NACK, MessageType.OTA_C2D, first[2].toInt())
        advanceTimeBy(999); runCurrent()
        assertEquals("backs off before retransmitting", 1, peripheral.rxWrites.size)
        advanceTimeBy(2); answerAll()

        assertEquals(2, peripheral.rxWrites.size)
        assertArrayEquals(first, peripheral.rxWrites[1])
        control(MessageType.ACK, MessageType.OTA_C2D, first[2].toInt())
        assertTrue(sending.await().isSuccess)
    }

    @Test
    fun `no answer retransmits after the ACK timeout`() = runTest {
        prepared()
        val sending = sendOta(byteArrayOf(6))

        advanceTimeBy(SpotflowGattSession.ACK_TIMEOUT_MS + 1_001); answerAll()

        assertEquals(2, peripheral.rxWrites.size)
        assertArrayEquals(peripheral.rxWrites[0], peripheral.rxWrites[1])
        control(MessageType.ACK, MessageType.OTA_C2D, peripheral.rxWrites[0][2].toInt())
        assertTrue(sending.await().isSuccess)
    }

    @Test
    fun `the next acknowledged message of a type gets the next sequence number`() = runTest {
        prepared()
        val first = sendOta(byteArrayOf(1))
        control(MessageType.ACK, MessageType.OTA_C2D, peripheral.rxWrites[0][2].toInt())
        first.await()
        val second = sendOta(byteArrayOf(2))

        assertEquals((peripheral.rxWrites[0][2] + 1).toByte(), peripheral.rxWrites[1][2])
        control(MessageType.ACK, MessageType.OTA_C2D, peripheral.rxWrites[1][2].toInt())
        assertTrue(second.await().isSuccess)
    }

    @Test
    fun `a message waiting for its ACK fails when the link goes down`() = runTest {
        prepared()
        val sending = sendOta(byteArrayOf(7))

        session.callback.onConnectionStateChange(peripheral.gatt, 8, BluetoothProfile.STATE_DISCONNECTED)
        runCurrent()

        assertTrue(sending.await().isFailure)
    }

    @Test
    fun `control frames never reach the incoming messages`() = runTest {
        prepared()
        val received = async { session.incoming.first() }
        runCurrent()

        control(MessageType.ACK, MessageType.OTA_C2D, 0)
        control(MessageType.NACK, MessageType.OTA_C2D, 0)
        FrameCodec.fragment(MessageType.OTA_D2C, byteArrayOf(9, 9), seq = 0, mtu = 247)
            .forEach { peripheral.notify(session.callback, peripheral.tx, it) }
        runCurrent()

        assertEquals(Message(MessageType.OTA_D2C, byteArrayOf(9, 9)), received.await())
    }
}

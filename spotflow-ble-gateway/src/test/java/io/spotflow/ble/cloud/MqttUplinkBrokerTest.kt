package io.spotflow.ble.cloud

import io.spotflow.ble.cloud.ScriptedBroker.Reply
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Runs the real [MqttUplink] — and so the real HiveMQ client — against a [ScriptedBroker]. The
 * orchestration tests use a fake uplink; these tests are what makes that fake an honest stand-in, by
 * holding the real one to the [Uplink] contract in the situations where the client's own behaviour
 * differs from it (it keeps connecting after a cancel, and keeps publishes across a lost connection).
 */
class MqttUplinkBrokerTest {

    private fun uplink(broker: ScriptedBroker) = MqttUplink(
        "dev", StaticIngestKey("key"), MqttConfig(host = "127.0.0.1", port = broker.port, useTls = false),
    )

    /** Waits (real time) until [condition] holds; fails the test if it doesn't within [timeoutMs]. */
    private suspend fun eventually(what: String, timeoutMs: Long = 5_000, condition: () -> Boolean) {
        val deadline = System.nanoTime() + timeoutMs * 1_000_000
        while (!condition()) {
            if (System.nanoTime() > deadline) throw AssertionError("timed out waiting until $what")
            delay(20)
        }
    }

    private val telemetry = byteArrayOf(1, 2, 3)

    @Test
    fun `is connected only once subscribed`() = runBlocking {
        ScriptedBroker().use { broker ->
            val uplink = uplink(broker)
            assertFalse(uplink.isConnected)

            uplink.connect()

            assertTrue(uplink.isConnected)
            assertEquals(listOf("c1:CONNECT", "c1:CONNACK", "c1:SUBSCRIBE"), broker.events.toList())
            uplink.disconnect()
            assertFalse(uplink.isConnected)
        }
    }

    @Test
    fun `a cancelled connect is cancelled, and leaves no connection behind`() = runBlocking {
        ScriptedBroker(connAckDelayMs = 600).use { broker ->
            val uplink = uplink(broker)
            var thrown: Throwable? = null
            val connecting = launch(Dispatchers.IO) {
                try {
                    uplink.connect()
                } catch (t: Throwable) {
                    thrown = t
                    throw t
                }
            }
            eventually("the broker has the CONNECT") { broker.has("c1:CONNECT") }
            connecting.cancelAndJoin()

            assertTrue("cancellation must not be reported as a failed connect: $thrown", thrown is CancellationException)
            assertFalse(uplink.isConnected)

            // The client completes the attempt regardless; the uplink must hang up on it, not keep it as a
            // live connection that never subscribed.
            eventually("the abandoned connection is closed") { broker.has("c1:DISCONNECT") }
            assertFalse(broker.has("c1:SUBSCRIBE"))
            assertFalse(uplink.isConnected)

            uplink.connect() // and the uplink is usable again
            assertTrue(uplink.isConnected)
            assertTrue(broker.has("c2:SUBSCRIBE"))
            uplink.disconnect()
        }
    }

    @Test
    fun `a connection lost mid-publish fails the publish at once, and it is not re-sent behind the caller's back`() =
        runBlocking {
            ScriptedBroker(
                sessionPresent = { it > 1 }, // the broker resumes the session, which is when the client would re-send
                onPublish = { connection -> if (connection == 1) Reply.Close else Reply.Ack },
            ).use { broker ->
                val uplink = uplink(broker)
                uplink.connect()

                val started = System.nanoTime()
                val error = runCatching { withTimeout(10_000) { uplink.publish("ingest-cbor", telemetry) } }.exceptionOrNull()
                val elapsedMs = (System.nanoTime() - started) / 1_000_000

                assertTrue("expected UplinkDisconnectedException but was $error", error is UplinkDisconnectedException)
                assertTrue("the caller is told right away, not after a timeout (took $elapsedMs ms)", elapsedMs < 3_000)
                assertFalse(uplink.isConnected)

                uplink.connect()
                delay(500) // time for a re-send, if the client were to make one
                assertEquals("the caller owns retransmission", 0, broker.events.count { it.startsWith("c2:PUBLISH") })

                uplink.publish("ingest-cbor", telemetry)
                assertEquals(listOf("c2:PUBLISH 1,2,3"), broker.events.filter { it.startsWith("c2:PUBLISH") })
                uplink.disconnect()
            }
        }

    @Test
    fun `a broker that hangs up on a publish is told apart from a broker with reasons of its own`() = runBlocking {
        suspend fun publishInto(reply: Reply): UplinkDisconnectedException =
            ScriptedBroker(onPublish = { reply }).use { broker ->
                val uplink = uplink(broker)
                uplink.connect()
                val error = runCatching { withTimeout(10_000) { uplink.publish("ingest-cbor", telemetry) } }.exceptionOrNull()
                assertTrue("expected UplinkDisconnectedException but was $error", error is UplinkDisconnectedException)
                error as UplinkDisconnectedException
            }

        assertTrue("PACKET_TOO_LARGE blames the message", publishInto(Reply.Disconnect(0x95)).rejectedByBroker)
        assertFalse("SESSION_TAKEN_OVER does not", publishInto(Reply.Disconnect(0x8E)).rejectedByBroker)
        assertFalse("nor does SERVER_SHUTTING_DOWN", publishInto(Reply.Disconnect(0x8B)).rejectedByBroker)
        assertTrue("hanging up without a word is taken as a rejection", publishInto(Reply.Close).rejectedByBroker)
    }

    @Test
    fun `a refused publish fails with the broker's answer while the connection stays up`() = runBlocking {
        ScriptedBroker(onPublish = { Reply.Refuse(0x87) }).use { broker -> // NOT_AUTHORIZED
            val uplink = uplink(broker)
            uplink.connect()

            val error = runCatching { withTimeout(10_000) { uplink.publish("ingest-cbor", telemetry) } }.exceptionOrNull()

            assertTrue(
                "expected the broker's refusal but was $error",
                error != null && error !is UplinkDisconnectedException && error !is UplinkThrottledException,
            )
            assertTrue(uplink.isConnected)
            uplink.disconnect()
        }
    }

    @Test
    fun `a publish refused for quota is reported as throttling, not as a bad message`() = runBlocking {
        ScriptedBroker(onPublish = { Reply.Refuse(0x97) }).use { broker -> // QUOTA_EXCEEDED
            val uplink = uplink(broker)
            uplink.connect()

            val error = runCatching { withTimeout(10_000) { uplink.publish("ingest-cbor", telemetry) } }.exceptionOrNull()

            assertTrue("expected UplinkThrottledException but was $error", error is UplinkThrottledException)
            assertTrue(uplink.isConnected)
            uplink.disconnect()
        }
    }

    @Test
    fun `a connection lost before the subscription is confirmed fails the connect instead of hanging`() = runBlocking {
        ScriptedBroker(cutOnSubscribe = { it == 1 }).use { broker ->
            val uplink = uplink(broker)

            val started = System.nanoTime()
            val error = runCatching { withTimeout(10_000) { uplink.connect() } }.exceptionOrNull()
            val elapsedMs = (System.nanoTime() - started) / 1_000_000

            assertTrue("expected MqttConnectException but was $error", error is MqttConnectException)
            assertTrue("failed right away (took $elapsedMs ms)", elapsedMs < 3_000)
            assertFalse(uplink.isConnected)

            uplink.connect() // and the next attempt works
            assertTrue(uplink.isConnected)
            uplink.disconnect()
        }
    }

    @Test
    fun `publishes in flight together reach the broker in the order they were started`() = runBlocking {
        ScriptedBroker().use { broker ->
            val uplink = uplink(broker)
            uplink.connect()

            (1..8).map { n -> async(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) { uplink.publish("ingest-cbor", byteArrayOf(n.toByte())) } }
                .awaitAll()

            assertEquals((1..8).map { "c1:PUBLISH $it" }, broker.events.filter { it.startsWith("c1:PUBLISH") })
            uplink.disconnect()
        }
    }

    @Test
    fun `a lost connection is announced, a deliberate disconnect is not`() = runBlocking {
        ScriptedBroker().use { broker ->
            val uplink = uplink(broker)
            val lost = CopyOnWriteArrayList<Unit>()
            uplink.onConnectionLost = { lost += Unit }

            uplink.connect()
            broker.cut(1)
            eventually("the lost connection is announced") { lost.size == 1 }
            assertFalse(uplink.isConnected)

            uplink.connect()
            uplink.disconnect()
            delay(200)
            assertEquals(1, lost.size)
        }
    }

    @Test
    fun `desired configuration is handed over and acknowledged only when the handler says so`() = runBlocking {
        // On a per-device topic below the subscribed one, as the Spotflow broker sends it.
        ScriptedBroker(publishOnSubscribe = "config-cbor-c2d/workspace/dev" to byteArrayOf(7, 7)).use { broker ->
            val uplink = uplink(broker)
            val received = CopyOnWriteArrayList<Pair<ByteArray, () -> Unit>>()
            uplink.desiredConfigurationHandler = { payload, ack -> received += payload to ack }

            uplink.connect()
            eventually("the configuration arrives") { received.size == 1 }
            assertTrue(received[0].first.contentEquals(byteArrayOf(7, 7)))
            delay(200)
            assertFalse("not acknowledged before it is written to the device", broker.has("c1:PUBACK"))

            received[0].second()
            eventually("the acknowledgement reaches the broker") { broker.has("c1:PUBACK") }
            uplink.disconnect()
        }
    }

    @Test
    fun `a rejected key is an auth failure, other refusals are retryable`() = runBlocking {
        ScriptedBroker(connAckReason = { 0x86 }).use { broker -> // BAD_USER_NAME_OR_PASSWORD
            val error = runCatching { uplink(broker).connect() }.exceptionOrNull()
            assertTrue("expected MqttAuthException but was $error", error is MqttAuthException)
        }
        ScriptedBroker(connAckReason = { 0x88 }).use { broker -> // SERVER_UNAVAILABLE
            val uplink = uplink(broker)
            val error = runCatching { uplink.connect() }.exceptionOrNull()
            assertTrue("expected MqttConnectException but was $error", error is MqttConnectException)
            assertFalse(uplink.isConnected)
        }
    }

    @Test
    fun `an unreachable broker is a retryable failure`() = runBlocking {
        val port = ScriptedBroker().use { it.port } // closed again: nothing listens there
        val uplink = MqttUplink("dev", StaticIngestKey("key"), MqttConfig(host = "127.0.0.1", port = port, useTls = false))

        val error = runCatching { uplink.connect() }.exceptionOrNull()

        assertTrue("expected MqttConnectException but was $error", error is MqttConnectException)
        assertFalse(uplink.isConnected)
    }

    @Test
    fun `OTA commands are subscribed to on request, and reach their own handler`() = runBlocking {
        val command = "ota-cbor-c2d/workspace/dev" to byteArrayOf(5, 5)
        ScriptedBroker(publishOnSubscribeTo = mapOf("ota-cbor-c2d" to command)).use { broker ->
            val uplink = uplink(broker)
            val commands = CopyOnWriteArrayList<Pair<ByteArray, () -> Unit>>()
            var desired = 0
            uplink.otaCommandHandler = { payload, ack -> commands += payload to ack }
            uplink.desiredConfigurationHandler = { _, _ -> desired++ }

            uplink.connect()
            assertFalse(uplink.isOtaSubscribed)
            uplink.subscribeOta()
            assertTrue(uplink.isOtaSubscribed)
            assertEquals(listOf("c1:+config-cbor-c2d", "c1:+ota-cbor-c2d"), broker.subscriptions.toList())

            eventually("the command arrives") { commands.size == 1 }
            assertTrue(commands[0].first.contentEquals(byteArrayOf(5, 5)))
            assertEquals(0, desired)
            delay(200)
            assertFalse("not acknowledged before the device has it", broker.has("c1:PUBACK"))
            commands[0].second()
            eventually("the acknowledgement reaches the broker") { broker.has("c1:PUBACK") }

            // A new connection starts unsubscribed; the broker may still hold the old subscription.
            uplink.disconnect()
            uplink.connect()
            assertFalse(uplink.isOtaSubscribed)
            assertTrue(uplink.mayHoldOtaSubscription)
            uplink.unsubscribeOta()
            assertFalse(uplink.mayHoldOtaSubscription)
            assertEquals("c2:-ota-cbor-c2d", broker.subscriptions.last())
            uplink.disconnect()
        }
    }

    @Test
    fun `a refused OTA subscription fails, and leaves the connection up`() = runBlocking {
        ScriptedBroker(subAckReason = { if (it == "ota-cbor-c2d") 0x87 else 0 }).use { broker ->
            val uplink = uplink(broker)
            uplink.connect()

            assertTrue(runCatching { uplink.subscribeOta() }.isFailure)
            assertFalse(uplink.isOtaSubscribed)
            assertTrue(uplink.isConnected)
            uplink.disconnect()
        }
    }

    @Test
    fun `subscribing to OTA commands without a connection fails at once`() = runBlocking {
        ScriptedBroker().use { broker ->
            val error = runCatching { uplink(broker).subscribeOta() }.exceptionOrNull()
            assertTrue("$error", error is UplinkDisconnectedException)
        }
    }
}

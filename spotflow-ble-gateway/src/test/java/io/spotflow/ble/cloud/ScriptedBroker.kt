package io.spotflow.ble.cloud

import java.io.DataInputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

/**
 * A minimal MQTT 5 broker on a local socket that does exactly what a test scripts, so the real client can
 * be put through situations a real broker only produces by accident: a slow CONNACK, a connection that
 * dies with a publish in flight, a DISCONNECT with a given reason.
 *
 * Every packet it receives or sends is recorded in [events] as `c<connection>:<what>`, connections being
 * numbered from 1 in the order they were accepted.
 */
internal class ScriptedBroker(
    private val connAckDelayMs: Long = 0,
    /** CONNACK reason code; 0 accepts the connection. */
    private val connAckReason: (connection: Int) -> Int = { 0 },
    private val sessionPresent: (connection: Int) -> Boolean = { false },
    private val onPublish: (connection: Int) -> Reply = { Reply.Ack },
    /** Whether the connection is cut when the client subscribes, before any SUBACK. */
    private val cutOnSubscribe: (connection: Int) -> Boolean = { false },
    /** Sent to the client right after it subscribes: a topic and a payload, as desired configuration is. */
    private val publishOnSubscribe: Pair<String, ByteArray>? = null,
    /** The SUBACK reason code for a topic filter (0x87 = not authorized). */
    private val subAckReason: (topicFilter: String) -> Int = { 0 },
    /** Like [publishOnSubscribe], but only after subscribing to the given topic filter. */
    private val publishOnSubscribeTo: Map<String, Pair<String, ByteArray>> = emptyMap(),
) : AutoCloseable {

    /** What the broker does with a PUBLISH it receives. */
    sealed interface Reply {
        data object Ack : Reply

        /** PUBACK carrying an error reason code: the broker refuses the message, the connection stays up. */
        class Refuse(val reason: Int) : Reply

        /** No answer at all. */
        data object Ignore : Reply

        /** The connection is cut without a word, as a network failure would. */
        data object Close : Reply

        /** A DISCONNECT packet with this reason code, then the connection is closed. */
        class Disconnect(val reason: Int) : Reply
    }

    private val server = ServerSocket(0)
    val port: Int get() = server.localPort
    val events = CopyOnWriteArrayList<String>()

    /** "c<connection>:+<filter>" per subscription, "c<connection>:-<filter>" per unsubscription. */
    val subscriptions = CopyOnWriteArrayList<String>()
    private val sockets = CopyOnWriteArrayList<Socket>()

    @Volatile private var connections = 0

    init {
        thread(isDaemon = true, name = "scripted-broker") {
            try {
                while (true) {
                    val socket = server.accept()
                    val connection = ++connections
                    sockets += socket
                    thread(isDaemon = true, name = "scripted-broker-c$connection") { serve(connection, socket) }
                }
            } catch (_: Exception) {
                // closed
            }
        }
    }

    /** Cuts the given connection from the broker's side, as [Reply.Close] does for a publish. */
    fun cut(connection: Int) = sockets[connection - 1].close()

    fun count(event: String): Int = events.count { it == event }

    fun has(event: String): Boolean = event in events

    private fun serve(connection: Int, socket: Socket) {
        val tag = "c$connection"
        try {
            val input = DataInputStream(socket.getInputStream())
            val out = socket.getOutputStream()
            while (true) {
                val first = input.readUnsignedByte()
                var multiplier = 1
                var length = 0
                while (true) {
                    val byte = input.readUnsignedByte()
                    length += (byte and 0x7F) * multiplier
                    multiplier *= 128
                    if (byte and 0x80 == 0) break
                }
                val body = ByteArray(length)
                input.readFully(body)
                when (first shr 4) {
                    CONNECT -> {
                        events += "$tag:CONNECT"
                        Thread.sleep(connAckDelayMs)
                        val flags = if (sessionPresent(connection)) 1 else 0
                        out.send(0x20, 0x03, flags, connAckReason(connection), 0)
                        events += "$tag:CONNACK"
                    }
                    SUBSCRIBE -> {
                        events += "$tag:SUBSCRIBE"
                        val filter = topicFilter(body)
                        subscriptions += "$tag:+$filter"
                        if (cutOnSubscribe(connection)) {
                            socket.close()
                            continue
                        }
                        val reason = subAckReason(filter)
                        out.send(0x90, 0x04, body[0].toInt(), body[1].toInt(), 0, if (reason == 0) 1 else reason)
                        if (reason != 0) continue
                        (publishOnSubscribeTo[filter] ?: publishOnSubscribe)?.let { (topic, payload) ->
                            val topicBytes = topic.toByteArray()
                            val remaining = 2 + topicBytes.size + 2 + 1 + payload.size
                            check(remaining < 128) { "keep scripted publishes short" }
                            out.write(byteArrayOf(0x32, remaining.toByte(), 0, topicBytes.size.toByte()))
                            out.write(topicBytes)
                            out.write(byteArrayOf(0, 1, 0)) // packet ID 1, no properties
                            out.write(payload)
                            out.flush()
                        }
                    }
                    PUBLISH -> {
                        val dup = first and 0x08 != 0
                        val topicLength = ((body[0].toInt() and 0xFF) shl 8) or (body[1].toInt() and 0xFF)
                        val idHigh = body[2 + topicLength].toInt() and 0xFF
                        val idLow = body[3 + topicLength].toInt() and 0xFF
                        val payload = body.copyOfRange(2 + topicLength + 2 + 1, body.size) // no properties expected
                        events += "$tag:PUBLISH ${payload.joinToString(",")}${if (dup) " dup" else ""}"
                        when (val reply = onPublish(connection)) {
                            Reply.Ack -> out.send(0x40, 0x02, idHigh, idLow)
                            is Reply.Refuse -> out.send(0x40, 0x03, idHigh, idLow, reply.reason)
                            Reply.Ignore -> Unit
                            Reply.Close -> socket.close()
                            is Reply.Disconnect -> {
                                out.send(0xE0, 0x01, reply.reason)
                                socket.close()
                            }
                        }
                    }
                    UNSUBSCRIBE -> {
                        subscriptions += "$tag:-${topicFilter(body)}"
                        out.send(0xB0, 0x04, body[0].toInt(), body[1].toInt(), 0, 0)
                    }
                    PUBACK -> events += "$tag:PUBACK"
                    PINGREQ -> out.send(0xD0, 0)
                    DISCONNECT -> {
                        events += "$tag:DISCONNECT"
                        socket.close()
                        return
                    }
                    else -> events += "$tag:packet${first shr 4}"
                }
            }
        } catch (_: Exception) {
            events += "$tag:closed"
        }
    }

    /** The first topic filter of a SUBSCRIBE or UNSUBSCRIBE: after the packet ID and (short) properties. */
    private fun topicFilter(body: ByteArray): String {
        val at = 3 + body[2]
        val length = ((body[at].toInt() and 0xFF) shl 8) or (body[at + 1].toInt() and 0xFF)
        return String(body, at + 2, length)
    }

    private fun OutputStream.send(vararg bytes: Int) {
        write(ByteArray(bytes.size) { bytes[it].toByte() })
        flush()
    }

    override fun close() {
        server.close()
        sockets.forEach { runCatching { it.close() } }
    }

    private companion object {
        const val CONNECT = 1
        const val PUBLISH = 3
        const val PUBACK = 4
        const val SUBSCRIBE = 8
        const val UNSUBSCRIBE = 10
        const val PINGREQ = 12
        const val DISCONNECT = 14
    }
}

package io.spotflow.ble.protocol

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * The artifact transfer of protocol 1.1: how a device downloads an OTA artifact through the gateway.
 * Requests arrive as [MessageType.ARTIFACT_REQUEST] messages, responses go back as
 * [MessageType.ARTIFACT_RESPONSE]; both are ordinary framed messages, without `NEEDS_ACK` (the response is
 * the acknowledgement). Integers are unsigned little-endian.
 *
 * ```
 * Common header:    [0] operation  [1..4] transfer ID  [5..8] request ID
 * OPEN request:     [9..10] URL length  [11] secret length  URL bytes  secret bytes (UTF-8, no terminators)
 * READ request:     [9..12] absolute offset  [13..14] byte count (1..4096)
 * CLOSE request:    [9..12] consumed offset  [13] reason (0 complete, 1 interrupted or canceled)
 * Response:         header with the request's operation | 0x80 and its IDs  [9] status
 *   OPEN success:   [10..13] artifact size (positive)
 *   READ success:   [10..13] absolute offset  then exactly the requested bytes
 *   CLOSE success, and every error: nothing more (10 bytes)
 * ```
 */
internal object ArtifactProtocol {

    const val OP_OPEN = 0x01
    const val OP_READ = 0x02
    const val OP_CLOSE = 0x03
    private const val RESPONSE_FLAG = 0x80

    const val HEADER_SIZE = 9

    /** Largest block a READ may ask for. */
    const val MAX_READ = 4096

    /** Limits of the OPEN request's fields, from the protocol (and the OTA manifest they come from). */
    const val MAX_URL_BYTES = 128
    const val MAX_SECRET_BYTES = 24

    /** Response status. */
    enum class Status(val code: Int) {
        OK(0),
        INVALID_REQUEST(1),
        ACCESS_DENIED(2),
        TRANSIENT_FAILURE(3),
        TOO_LARGE(4),
    }

    /** What identifies a request — and so its response, and its repetitions. */
    data class Header(val operation: Int, val transferId: Long, val requestId: Long)

    sealed interface Request {
        val header: Header

        class Open(override val header: Header, val url: String, val secret: String) : Request {
            // Never print the URL or the secret: they authorize the download.
            override fun toString(): String = "Open(transfer ${header.transferId})"
        }

        data class Read(override val header: Header, val offset: Long, val count: Int) : Request

        data class Close(override val header: Header, val consumed: Long, val complete: Boolean) : Request

        /** A request with a readable header but a body that breaks the protocol: answered with an error. */
        data class Malformed(override val header: Header, val reason: String) : Request
    }

    /**
     * Parses a reassembled request. Returns null if not even the header can be read (such a request gets
     * no response: there is nothing to address it with).
     */
    fun parse(payload: ByteArray): Request? {
        if (payload.size < HEADER_SIZE) return null
        val buffer = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN)
        val header = Header(
            operation = buffer.get().toInt() and 0xFF,
            transferId = buffer.int.toLong() and 0xFFFFFFFFL,
            requestId = buffer.int.toLong() and 0xFFFFFFFFL,
        )
        fun malformed(reason: String) = Request.Malformed(header, reason)
        if (header.transferId == 0L) return malformed("transfer ID 0")
        return when (header.operation) {
            OP_OPEN -> {
                if (buffer.remaining() < 3) return malformed("OPEN too short")
                val urlLength = buffer.short.toInt() and 0xFFFF
                val secretLength = buffer.get().toInt() and 0xFF
                if (urlLength == 0 || urlLength > MAX_URL_BYTES) return malformed("URL length $urlLength")
                if (secretLength == 0 || secretLength > MAX_SECRET_BYTES) {
                    return malformed("secret length $secretLength")
                }
                if (buffer.remaining() != urlLength + secretLength) return malformed("OPEN length mismatch")
                val url = ByteArray(urlLength).also { buffer.get(it) }.toString(Charsets.UTF_8)
                val secretBytes = ByteArray(secretLength).also { buffer.get(it) }
                // A printable ASCII token: it goes into an HTTP header verbatim.
                if (secretBytes.any { it < 0x21 || it > 0x7E }) return malformed("secret is not a printable token")
                Request.Open(header, url, secretBytes.toString(Charsets.US_ASCII))
            }
            OP_READ -> {
                if (buffer.remaining() != 6) return malformed("READ length ${payload.size}")
                val offset = buffer.int.toLong() and 0xFFFFFFFFL
                val count = buffer.short.toInt() and 0xFFFF
                if (count == 0 || count > MAX_READ) return malformed("READ count $count")
                Request.Read(header, offset, count)
            }
            OP_CLOSE -> {
                if (buffer.remaining() != 5) return malformed("CLOSE length ${payload.size}")
                val consumed = buffer.int.toLong() and 0xFFFFFFFFL
                when (val reason = buffer.get().toInt() and 0xFF) {
                    0, 1 -> Request.Close(header, consumed, complete = reason == 0)
                    else -> malformed("CLOSE reason $reason")
                }
            }
            else -> malformed("operation ${header.operation}")
        }
    }

    /** A response with [status] and nothing more: every error, and a successful CLOSE. */
    fun status(header: Header, status: Status): ByteArray = response(header, status, 0).array()

    /** A successful OPEN: the artifact's size. */
    fun opened(header: Header, size: Long): ByteArray =
        response(header, Status.OK, 4).putInt(size.toInt()).array()

    /** A successful READ: the block at [offset]. */
    fun block(header: Header, offset: Long, data: ByteArray): ByteArray =
        response(header, Status.OK, 4 + data.size).putInt(offset.toInt()).put(data).array()

    private fun response(header: Header, status: Status, extra: Int): ByteBuffer =
        ByteBuffer.allocate(HEADER_SIZE + 1 + extra).order(ByteOrder.LITTLE_ENDIAN)
            .put((header.operation or RESPONSE_FLAG).toByte())
            .putInt(header.transferId.toInt())
            .putInt(header.requestId.toInt())
            .put(status.code.toByte())
}

package io.spotflow.ble.protocol

import java.io.ByteArrayOutputStream

/**
 * Encodes and decodes the Spotflow BLE framed protocol.
 *
 * A logical message may be larger than the negotiated ATT MTU (which can be as low as 23 bytes), so
 * messages are split into fragments. Every fragment starts with a 3-byte common header; the first
 * fragment of a message carries two extra bytes with the total message length:
 *
 * ```
 * First fragment:        [0] type  [1] flags  [2] seq  [3..4] totalLen (u16 LE)  [5..] payload
 * Continuation fragment: [0] type  [1] flags  [2] seq  [3..]  payload
 * ```
 *
 * All fragments of one message share the same `type` and `seq`; `seq` increments per message and is
 * how the [Reassembler] groups fragments. Flags mark the first and last fragment.
 *
 * NOTE: the exact numeric flag bit values below are not published in the Spotflow docs and must be
 * verified against the Device SDK before interop testing.
 */
object FrameCodec {

    /** Flag bits carried in byte [1] of every frame. */
    const val FLAG_IS_FIRST = 0x01
    const val FLAG_IS_LAST = 0x02
    const val FLAG_NEEDS_ACK = 0x04

    /** Header sizes in bytes. */
    const val HEADER_FIRST = 5
    const val HEADER_CONTINUATION = 3

    /** ATT protocol overhead: 3 bytes of every ATT payload are the opcode + handle. */
    private const val ATT_OVERHEAD = 3

    /** Smallest MTU the BLE spec guarantees. */
    const val MIN_MTU = 23

    const val MAX_MESSAGE_LENGTH = 0xFFFF

    /**
     * Splits [payload] of the given [type] into wire frames that each fit within [mtu].
     *
     * @param seq message sequence number (0..255), shared by every fragment of this message.
     * @return the ordered list of frames to write to the RX Stream characteristic. An empty payload
     *   still produces exactly one frame flagged both first and last.
     */
    fun fragment(type: MessageType, payload: ByteArray, seq: Int, mtu: Int): List<ByteArray> {
        require(payload.size <= MAX_MESSAGE_LENGTH) {
            "payload of ${payload.size} bytes exceeds max message length $MAX_MESSAGE_LENGTH"
        }
        require(seq in 0..0xFF) { "seq $seq out of range 0..255" }

        val attPayload = maxOf(mtu, MIN_MTU) - ATT_OVERHEAD
        val firstCapacity = attPayload - HEADER_FIRST
        val continuationCapacity = attPayload - HEADER_CONTINUATION
        require(firstCapacity > 0 && continuationCapacity > 0) { "mtu $mtu too small to frame a message" }

        val total = payload.size
        val frames = ArrayList<ByteArray>()
        var offset = 0
        var isFirst = true

        do {
            val capacity = if (isFirst) firstCapacity else continuationCapacity
            val chunk = minOf(capacity, total - offset)
            val isLast = offset + chunk >= total

            var flags = 0
            if (isFirst) flags = flags or FLAG_IS_FIRST
            if (isLast) flags = flags or FLAG_IS_LAST

            val headerLen = if (isFirst) HEADER_FIRST else HEADER_CONTINUATION
            val frame = ByteArray(headerLen + chunk)
            frame[0] = type.value.toByte()
            frame[1] = flags.toByte()
            frame[2] = seq.toByte()
            if (isFirst) {
                frame[3] = (total and 0xFF).toByte()
                frame[4] = ((total ushr 8) and 0xFF).toByte()
                payload.copyInto(frame, HEADER_FIRST, offset, offset + chunk)
            } else {
                payload.copyInto(frame, HEADER_CONTINUATION, offset, offset + chunk)
            }
            frames.add(frame)

            offset += chunk
            isFirst = false
        } while (offset < total)

        return frames
    }

    /**
     * Reassembles inbound fragments (from TX Stream notifications) into complete [Message]s.
     *
     * A malformed, orphaned, or incomplete fragment sequence is dropped (returns null) rather than
     * throwing, so one bad notification cannot tear down the stream. A message is only delivered if its
     * reassembled size matches the length declared in its first fragment — a lost fragment must not turn
     * into a silently truncated (corrupt) payload being forwarded to the cloud. Memory is bounded: a
     * partial never grows past its declared length, and at most [MAX_PARTIALS] messages are in flight.
     */
    class Reassembler {

        private data class Key(val type: Int, val seq: Int)

        private class Partial(val expectedLength: Int) {
            val buffer = ByteArrayOutputStream(expectedLength)
        }

        // Insertion-ordered so the oldest in-flight partial is evicted first when over the cap.
        private val partials = object : LinkedHashMap<Key, Partial>() {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Key, Partial>) = size > MAX_PARTIALS
        }

        /**
         * Feeds one raw notification frame. Returns a [Message] when a message completes, else null.
         *
         * Synchronized because it is called on the BLE binder thread while [reset] may be called from a
         * coroutine during teardown.
         */
        @Synchronized
        fun onFragment(raw: ByteArray): Message? {
            if (raw.size < HEADER_CONTINUATION) return null

            val rawType = raw[0].toInt() and 0xFF
            val flags = raw[1].toInt() and 0xFF
            val seq = raw[2].toInt() and 0xFF
            val isFirst = flags and FLAG_IS_FIRST != 0
            val isLast = flags and FLAG_IS_LAST != 0
            val key = Key(rawType, seq)

            val partial: Partial
            val dataStart: Int
            if (isFirst) {
                if (raw.size < HEADER_FIRST) return null
                val expected = (raw[3].toInt() and 0xFF) or ((raw[4].toInt() and 0xFF) shl 8)
                partial = Partial(expected)
                partials[key] = partial
                dataStart = HEADER_FIRST
            } else {
                partial = partials[key] ?: return null // continuation without a first fragment
                dataStart = HEADER_CONTINUATION
            }

            val chunk = raw.size - dataStart
            if (partial.buffer.size() + chunk > partial.expectedLength) {
                // More data than the first fragment declared: a corrupt or hostile stream. Drop it
                // rather than buffering without bound.
                partials.remove(key)
                return null
            }
            partial.buffer.write(raw, dataStart, chunk)

            if (!isLast) return null

            partials.remove(key)
            if (partial.buffer.size() != partial.expectedLength) return null // a fragment was lost
            val type = MessageType.fromValue(rawType) ?: return null // unknown type: drop
            return Message(type, partial.buffer.toByteArray())
        }

        /** Drops any in-flight partial messages (e.g. after a disconnect). */
        @Synchronized
        fun reset() = partials.clear()

        private companion object {
            /** Max messages reassembled concurrently; a well-behaved device interleaves very few. */
            const val MAX_PARTIALS = 8
        }
    }
}

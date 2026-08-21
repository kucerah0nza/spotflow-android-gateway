package io.spotflow.ble.protocol

/**
 * A fully reassembled Spotflow BLE message. The [payload] is relayed opaquely (CBOR) to/from the
 * Spotflow cloud; this library does not parse it.
 */
class Message(
    val type: MessageType,
    val payload: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Message) return false
        return type == other.type && payload.contentEquals(other.payload)
    }

    override fun hashCode(): Int = 31 * type.hashCode() + payload.contentHashCode()

    override fun toString(): String = "Message(type=$type, payload=${payload.size} bytes)"
}

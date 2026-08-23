package io.spotflow.ble.cloud

import java.io.Closeable

/**
 * A two-tier store-and-forward buffer: an in-memory (RAM) tier in front of a persistent ([disk]) tier.
 *
 * Goal: avoid wearing the flash in normal operation. While the uplink keeps up, messages flow through
 * RAM only and are never written to flash. Only once the RAM tier exceeds [ramMaxBytes] — i.e. the
 * network has been unavailable long enough to build a backlog — does the buffer spill the oldest RAM
 * messages to disk (and evict the oldest of all when the disk tier is also full). The disk tier is only
 * touched during and after outages.
 *
 * Every item gets a monotonic sequence number that follows it across tiers, so FIFO order is preserved
 * globally (the disk tier always holds strictly lower sequence numbers than RAM and is drained first) and
 * a published item can be removed exactly, even if it was spilled to disk mid-publish. Data still in RAM
 * is lost if the process is killed — an accepted trade-off.
 *
 * Single-producer ([enqueue]) / single-consumer ([takeNext] then [remove]); all operations synchronized.
 * [takeNext] only *peeks*, so retries need no bookkeeping ([requeue] is a no-op) and there is neither
 * reordering nor duplication.
 */
internal class StoreAndForwardBuffer(
    private val disk: PersistentMessageQueue,
    private val ramMaxBytes: Long,
) : Closeable {

    /** A buffered message and its buffer-wide sequence number. */
    class Item(val seq: Long, val topic: String, val payload: ByteArray)

    private val lock = Any()
    private val ram = ArrayDeque<Item>()
    private var ramTierBytes = 0L
    private var nextSeq = disk.maxSeq() + 1 // continue after anything already persisted

    /** Bytes currently held in the in-memory tier. */
    val ramBytes: Long get() = synchronized(lock) { ramTierBytes }

    /** Bytes currently held in the persistent (flash) tier. */
    val diskBytes: Long get() = synchronized(lock) { disk.bytes }

    /** Total buffered bytes across both tiers. */
    val bytes: Long get() = synchronized(lock) { ramTierBytes + disk.bytes }

    /** Appends a message to RAM, spilling the oldest RAM messages to disk if RAM is over its cap. */
    fun enqueue(topic: String, payload: ByteArray) = synchronized(lock) {
        ram.addLast(Item(nextSeq++, topic, payload))
        ramTierBytes += payload.size
        while (ramTierBytes > ramMaxBytes && ram.size > 1) {
            val oldest = ram.removeFirst()
            ramTierBytes -= oldest.payload.size
            disk.enqueue(oldest.seq, oldest.topic, oldest.payload)
        }
    }

    /**
     * Returns the oldest item to publish (disk tier first for FIFO), or null if empty. The item is only
     * peeked — finalize with [remove] after a successful publish; a failed publish needs no action.
     * `disk.bytes` is an in-memory counter, so the empty-disk fast path never queries SQLite.
     */
    fun takeNext(): Item? = synchronized(lock) {
        if (disk.bytes > 0L) {
            disk.peek()?.let { Item(it.seq, it.topic, it.payload) }
        } else {
            ram.firstOrNull()
        }
    }

    /** Removes a published (or intentionally dropped) item by sequence number, from whichever tier holds it. */
    fun remove(item: Item) = synchronized(lock) {
        disk.remove(item.seq) // no-op if the item never spilled
        val head = ram.firstOrNull()
        if (head != null && head.seq == item.seq) {
            ram.removeFirst()
            ramTierBytes -= head.payload.size
        }
    }

    /** No-op: [takeNext] only peeks, so a failed item is still queued at its position and will be retried. */
    @Suppress("UNUSED_PARAMETER")
    fun requeue(item: Item) = Unit

    /**
     * Moves all in-memory items to the persistent tier — call before tearing a session down so unsent
     * data survives a reconnect (e.g. the device restarting). Only writes to flash when RAM is non-empty.
     */
    fun flushToDisk() = synchronized(lock) {
        while (ram.isNotEmpty()) {
            val item = ram.removeFirst()
            ramTierBytes -= item.payload.size
            disk.enqueue(item.seq, item.topic, item.payload)
        }
    }

    override fun close() = synchronized(lock) { disk.close() }
}

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
 * FIFO across tiers is preserved: the disk tier always holds strictly older messages than RAM, so it is
 * drained first. Data still in RAM is lost if the process is killed — an accepted trade-off.
 *
 * Single-producer ([enqueue]) / single-consumer ([takeNext] then [remove]/[requeue]); all operations are
 * synchronized. A RAM item is popped by [takeNext] (so a concurrent spill can't move it); a disk item is
 * only peeked and is removed by [remove] after a successful publish (at-least-once for the durable tier).
 */
internal class StoreAndForwardBuffer(
    private val disk: PersistentMessageQueue,
    private val ramMaxBytes: Long,
) : Closeable {

    /** A buffered message. [diskId] is non-null for items taken from the disk tier. */
    class Item(val topic: String, val payload: ByteArray, internal val diskId: Long?)

    private val lock = Any()
    private val ram = ArrayDeque<Item>()
    private var ramTierBytes = 0L

    /** Bytes currently held in the in-memory tier. */
    val ramBytes: Long get() = synchronized(lock) { ramTierBytes }

    /** Bytes currently held in the persistent (flash) tier. */
    val diskBytes: Long get() = synchronized(lock) { disk.bytes }

    /** Total buffered bytes across both tiers. */
    val bytes: Long get() = synchronized(lock) { ramTierBytes + disk.bytes }

    /** Appends a message to RAM, spilling the oldest RAM messages to disk if RAM is over its cap. */
    fun enqueue(topic: String, payload: ByteArray) = synchronized(lock) {
        ram.addLast(Item(topic, payload, diskId = null))
        ramTierBytes += payload.size
        while (ramTierBytes > ramMaxBytes && ram.size > 1) {
            val oldest = ram.removeFirst()
            ramTierBytes -= oldest.payload.size
            disk.enqueue(oldest.topic, oldest.payload)
        }
    }

    /**
     * Returns the next item to publish (disk tier first for FIFO), or null if empty. Disk items remain on
     * disk until [remove]; RAM items are popped here (finalize with [remove] on success, [requeue] on
     * failure). `disk.bytes` is an in-memory counter, so the empty-disk fast path never queries SQLite.
     */
    fun takeNext(): Item? = synchronized(lock) {
        if (disk.bytes > 0L) {
            disk.peek()?.let { Item(it.topic, it.payload, diskId = it.id) }
        } else {
            ram.removeFirstOrNull()?.also { ramTierBytes -= it.payload.size }
        }
    }

    /** Finalizes removal after a successful publish (or an intentional drop). */
    fun remove(item: Item) = synchronized(lock) {
        if (item.diskId != null) disk.remove(item.diskId)
        // RAM items were already popped in takeNext().
    }

    /**
     * Moves all in-memory items to the persistent tier — call before tearing a session down so unsent
     * data survives a reconnect (e.g. the device restarting). Only writes to flash when RAM is non-empty.
     */
    fun flushToDisk() = synchronized(lock) {
        while (ram.isNotEmpty()) {
            val item = ram.removeFirst()
            ramTierBytes -= item.payload.size
            disk.enqueue(item.topic, item.payload)
        }
    }

    /** Returns an item to the buffer after a failed publish so it will be retried. */
    fun requeue(item: Item) = synchronized(lock) {
        if (item.diskId == null) {
            ram.addFirst(item)
            ramTierBytes += item.payload.size
        }
        // Disk items were only peeked, so they are still queued.
    }

    override fun close() = synchronized(lock) { disk.close() }
}

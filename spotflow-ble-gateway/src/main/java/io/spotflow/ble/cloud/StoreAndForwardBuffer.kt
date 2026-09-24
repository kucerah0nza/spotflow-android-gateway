package io.spotflow.ble.cloud

import android.util.Log
import java.io.Closeable

/**
 * A two-tier store-and-forward buffer: an in-memory (RAM) tier in front of an optional persistent
 * ([disk]) tier.
 *
 * Goal: avoid wearing the flash in normal operation. While the uplink keeps up, messages flow through
 * RAM only and are never written to flash. Only once the RAM tier exceeds [ramMaxBytes] — i.e. the
 * network has been unavailable long enough to build a backlog — does the buffer spill the oldest RAM
 * messages to disk (and evict the oldest of all when the disk tier is also full). Spills go down to half
 * of [ramMaxBytes] in one batch/transaction, so flash is written in occasional batches rather than on
 * every message during an outage.
 *
 * With no [disk] tier (flash size 0) the buffer is RAM-only: when RAM is full the oldest message is
 * dropped, and nothing is ever written to flash.
 *
 * Every item gets a monotonic sequence number that follows it across tiers, so FIFO order is preserved
 * globally (the disk tier always holds strictly lower sequence numbers than RAM and is drained first) and
 * a published item can be removed exactly, even if it was spilled to disk mid-publish. Data still in RAM
 * is lost if the process is killed — an accepted trade-off.
 *
 * Single-producer ([enqueue]) / single-consumer ([takeNext] then [remove]); all operations synchronized.
 * [takeNext] only *peeks*, so a failed publish needs no bookkeeping and there is neither reordering nor
 * duplication.
 */
internal class StoreAndForwardBuffer(
    private val disk: PersistentMessageQueue?,
    private val ramMaxBytes: Long,
) : Closeable {

    /** A buffered message and its buffer-wide sequence number. */
    class Item(val seq: Long, val topic: String, val payload: ByteArray)

    private val lock = Any()
    private val ram = ArrayDeque<Item>()
    private var ramTierBytes = 0L
    private var nextSeq = (disk?.maxSeq() ?: 0L) + 1 // continue after anything already persisted

    /** Bytes currently held in the in-memory tier. */
    val ramBytes: Long get() = synchronized(lock) { ramTierBytes }

    /** Bytes currently held in the persistent (flash) tier. */
    val diskBytes: Long get() = synchronized(lock) { disk?.bytes ?: 0L }

    /** Total buffered bytes across both tiers. */
    val bytes: Long get() = synchronized(lock) { ramTierBytes + (disk?.bytes ?: 0L) }

    /** Whether both tiers are empty. */
    val isEmpty: Boolean get() = synchronized(lock) { ram.isEmpty() && (disk?.isEmpty ?: true) }

    /** Appends a message to RAM, spilling (or, RAM-only, dropping) the oldest if RAM is over its cap. */
    fun enqueue(topic: String, payload: ByteArray): Unit = synchronized(lock) {
        ram.addLast(Item(nextSeq++, topic, payload))
        ramTierBytes += payload.size
        if (ramTierBytes <= ramMaxBytes || ram.size <= 1) return

        if (disk == null) {
            while (ramTierBytes > ramMaxBytes && ram.size > 1) dropOldestRam()
            return
        }
        val batch = ArrayList<Item>()
        var batchBytes = 0L
        val target = ramMaxBytes / 2
        for (item in ram) {
            if (ramTierBytes - batchBytes <= target || ram.size - batch.size <= 1) break
            batch += item
            batchBytes += item.payload.size
        }
        if (spill(batch)) {
            repeat(batch.size) { ram.removeFirst() }
            ramTierBytes -= batchBytes
        } else {
            // Storage is failing (e.g. full): keep RAM bounded by dropping the oldest instead.
            while (ramTierBytes > ramMaxBytes && ram.size > 1) dropOldestRam()
        }
    }

    /**
     * Returns the oldest item to publish (disk tier first for FIFO), or null if empty. The item is only
     * peeked — finalize with [remove] after a successful publish; a failed publish needs no action.
     * `disk.isEmpty` is an in-memory counter, so the empty-disk fast path never queries SQLite.
     */
    fun takeNext(): Item? = synchronized(lock) {
        if (disk != null && !disk.isEmpty) {
            disk.peek()?.let { return Item(it.seq, it.topic, it.payload) }
        }
        ram.firstOrNull()
    }

    /** Removes a published (or intentionally dropped) item by sequence number, from whichever tier holds it. */
    fun remove(item: Item): Unit = synchronized(lock) {
        val head = ram.firstOrNull()
        if (head != null && head.seq == item.seq) {
            ram.removeFirst()
            ramTierBytes -= head.payload.size
        } else {
            disk?.remove(item.seq) // the item was spilled (possibly mid-publish)
        }
    }

    /**
     * Moves all in-memory items to the persistent tier — call before closing so unsent data survives a
     * process restart. Only writes to flash when RAM is non-empty; a no-op without a disk tier.
     */
    fun flushToDisk(): Unit = synchronized(lock) {
        if (disk == null || ram.isEmpty()) return
        if (spill(ram.toList())) {
            ram.clear()
            ramTierBytes = 0
        }
    }

    /** Closes the disk tier, deleting its file if everything was delivered. */
    override fun close(): Unit = synchronized(lock) {
        disk?.close(deleteIfEmpty = ram.isEmpty())
    }

    private fun spill(items: List<Item>): Boolean =
        disk!!.enqueueAll(items.map { PersistentMessageQueue.Entry(it.seq, it.topic, it.payload) })

    private fun dropOldestRam() {
        val dropped = ram.removeFirst()
        ramTierBytes -= dropped.payload.size
        Log.w(TAG, "buffer full; dropped oldest message (${dropped.payload.size} bytes)")
    }

    private companion object {
        const val TAG = "SpotflowBuffer"
    }
}

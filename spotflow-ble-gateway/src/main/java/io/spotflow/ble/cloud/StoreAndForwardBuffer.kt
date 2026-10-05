package io.spotflow.ble.cloud

import android.util.Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.io.Closeable

/** How much of the gateway-wide store-and-forward budget is in use, across all devices. */
data class BufferUsage(
    val ramBytes: Long = 0,
    val ramMaxBytes: Long = 0,
    val flashBytes: Long = 0,
    /** 0 when the buffer is RAM-only. */
    val flashMaxBytes: Long = 0,
)

/**
 * The gateway-wide store-and-forward budget: every device's [StoreAndForwardBuffer] draws on one RAM
 * budget ([ramMaxBytes]) and one flash budget (the [store]'s limit), so the gateway's footprint is
 * bounded no matter how many devices it relays.
 *
 * Goal: avoid wearing the flash in normal operation. While the uplinks keep up, messages flow through RAM
 * only and are never written to flash. Only once RAM across all devices exceeds [ramMaxBytes] — i.e. the
 * network has been unavailable long enough to build a backlog — does the pool spill the oldest messages
 * of the device holding the most RAM to flash, down to half of [ramMaxBytes] in one batch/transaction, so
 * flash is written in occasional batches rather than on every message during an outage. When flash is
 * full too, the store evicts the oldest messages of the device holding the most flash.
 *
 * Fairness: whoever holds the most gives up its oldest data first, so a device that floods the buffer or
 * stays offline for long can't push out a quieter device's data.
 *
 * With no flash budget ([flashMaxBytes] 0) the pool is RAM-only: when RAM is full the oldest message of the
 * device holding the most RAM is dropped, and nothing is written to flash. The [store] is still read, so
 * data left on flash by an earlier run is delivered.
 *
 * All buffers share this pool's [lock]. [release] is called once, after [shutdown], when the last buffer
 * has closed.
 */
internal class BufferPool(
    private val store: MessageStore,
    private val ramMaxBytes: Long,
    private val flashMaxBytes: Long,
    private val release: () -> Unit = {},
) {
    internal val spillToFlash: Boolean = flashMaxBytes > 0

    internal val lock = Any()
    private val buffers = LinkedHashSet<StoreAndForwardBuffer>()
    private var ramUsed = 0L
    private var shutDown = false
    private var released = false

    /** Bytes held in RAM across all devices. */
    val ramBytes: Long get() = synchronized(lock) { ramUsed }

    /** Bytes held in flash across all devices. */
    val diskBytes: Long get() = store.bytes

    private val _usage = MutableStateFlow(BufferUsage(0, ramMaxBytes, store.bytes, flashMaxBytes))

    /** Live usage of the budget, updated on every change. */
    val usage: StateFlow<BufferUsage> = _usage

    /** Must hold [lock]. Publishes the current [usage]. */
    internal fun publishUsage() {
        _usage.value = BufferUsage(ramUsed, ramMaxBytes, store.bytes, flashMaxBytes)
    }

    /** Devices with data in flash, e.g. left behind when the app was killed during an outage. */
    fun storedDeviceIds(): List<String> = store.storedDeviceIds()

    /** Opens a buffer for [deviceId]; it first delivers anything the device already has stored. */
    fun open(deviceId: String): StoreAndForwardBuffer = synchronized(lock) {
        check(!released) { "buffer pool is closed" }
        StoreAndForwardBuffer(this, store, deviceId).also { buffers += it }
    }

    /** No new buffers will be opened; [release] runs once the open ones have closed. */
    fun shutdown() = synchronized(lock) {
        shutDown = true
        releaseIfIdle()
    }

    /** Must hold [lock]. */
    internal fun ramChanged(delta: Long) {
        ramUsed += delta
    }

    /**
     * Brings RAM back within budget after [ramMaxBytes] was exceeded. Must hold [lock]. Returns the
     * buffers whose contents changed (spilled, dropped or evicted), so their owners can be told.
     */
    internal fun enforceRamBudget(): Set<StoreAndForwardBuffer> {
        if (ramUsed <= ramMaxBytes) return emptySet()
        val changed = HashSet<StoreAndForwardBuffer>()
        if (spillToFlash) {
            val target = ramMaxBytes / 2
            while (ramUsed > target) {
                val victim = largestInRam() ?: break
                changed += victim
                val evicted = victim.spillOldest(ramUsed - target)
                if (evicted == null) {
                    // Storage is failing (e.g. full): keep RAM bounded by dropping the oldest instead.
                    dropUntilWithin(ramMaxBytes, changed)
                    break
                }
                changed += buffersOf(evicted)
            }
        } else {
            dropUntilWithin(ramMaxBytes, changed)
        }
        return changed
    }

    /** Must hold [lock]. Returns the buffers of [deviceIds] (devices that lost data to flash eviction). */
    internal fun buffersOf(deviceIds: Set<String>): List<StoreAndForwardBuffer> =
        if (deviceIds.isEmpty()) emptyList() else buffers.filter { it.deviceId in deviceIds }

    /** Must hold [lock]. */
    internal fun closed(buffer: StoreAndForwardBuffer) {
        buffers -= buffer
        releaseIfIdle()
    }

    private fun dropUntilWithin(limit: Long, changed: MutableSet<StoreAndForwardBuffer>) {
        // Keep the newest message overall, so one larger than the whole budget is still delivered.
        while (ramUsed > limit && buffers.sumOf { it.ramCount } > 1) {
            val victim = largestInRam() ?: break
            victim.dropOldestRam()
            changed += victim
        }
    }

    private fun largestInRam(): StoreAndForwardBuffer? =
        buffers.filter { it.ramCount > 0 }.maxByOrNull { it.ramTierBytes }

    private fun releaseIfIdle() {
        if (shutDown && buffers.isEmpty() && !released) {
            released = true
            release()
        }
    }
}

/**
 * One device's store-and-forward buffer: a RAM queue in front of the device's part of the shared flash
 * [store], within the budgets of its [pool] (see [BufferPool] for when data moves to flash or is evicted).
 *
 * Every item gets a monotonic, per-device sequence number (from the [store]) that follows it across tiers, so FIFO order is
 * preserved (the flash tier always holds strictly lower sequence numbers than RAM and is drained first)
 * and a published item can be removed exactly, even if it was spilled to flash mid-publish. Data still in
 * RAM is lost if the process is killed — an accepted trade-off.
 *
 * Single-producer ([enqueue]) / single-consumer ([takeNext] then [remove]). [takeNext] only *peeks*, so a
 * failed publish needs no bookkeeping and there is neither reordering nor duplication.
 */
internal class StoreAndForwardBuffer internal constructor(
    private val pool: BufferPool,
    private val store: MessageStore,
    val deviceId: String,
) : Closeable {

    /** A buffered message and its sequence number. */
    class Item(val seq: Long, val topic: String, val payload: ByteArray)

    /**
     * Called (outside the pool lock) when the pool changed this buffer's contents on behalf of another
     * device — spilled to flash or evicted — so its owner can refresh what it reports.
     */
    @Volatile var onExternalChange: (() -> Unit)? = null

    private val ram = ArrayDeque<Item>()

    /** Must hold the pool lock. */
    internal var ramTierBytes = 0L
        private set

    /** Must hold the pool lock. */
    internal val ramCount: Int get() = ram.size

    /** Bytes of this device held in RAM. */
    val ramBytes: Long get() = synchronized(pool.lock) { ramTierBytes }

    /** Bytes of this device held in flash. */
    val diskBytes: Long get() = store.bytes(deviceId)

    /** Total bytes of this device across both tiers. */
    val bytes: Long get() = synchronized(pool.lock) { ramTierBytes + store.bytes(deviceId) }

    /** Messages of this device waiting to be uploaded, across both tiers. */
    val count: Long get() = synchronized(pool.lock) { ram.size + store.count(deviceId) }

    /** Whether nothing of this device is buffered. */
    val isEmpty: Boolean get() = synchronized(pool.lock) { ram.isEmpty() && store.isEmpty(deviceId) }

    /** Appends a message to RAM; the pool then spills or drops as needed to stay within its budget. */
    fun enqueue(topic: String, payload: ByteArray) {
        val changed = synchronized(pool.lock) {
            ram.addLast(Item(store.nextSeq(deviceId), topic, payload))
            ramTierBytes += payload.size
            pool.ramChanged(payload.size.toLong())
            pool.enforceRamBudget().also { pool.publishUsage() }
        }
        notify(changed)
    }

    /**
     * Returns the oldest item to publish (flash first, for FIFO), or null if empty. The item is only
     * peeked — finalize with [remove] after a successful publish; a failed publish needs no action.
     * The flash check uses in-memory counters, so the common empty-flash path never queries SQLite.
     */
    fun takeNext(): Item? = synchronized(pool.lock) {
        if (!store.isEmpty(deviceId)) {
            store.peek(deviceId)?.let { return Item(it.seq, it.topic, it.payload) }
        }
        ram.firstOrNull()
    }

    /** Removes a published (or intentionally dropped) item, from whichever tier holds it. */
    fun remove(item: Item): Unit = synchronized(pool.lock) {
        val head = ram.firstOrNull()
        if (head != null && head.seq == item.seq) {
            ram.removeFirst()
            ramTierBytes -= head.payload.size
            pool.ramChanged(-head.payload.size.toLong())
        } else {
            store.remove(deviceId, item.seq) // the item was spilled (possibly mid-publish) or evicted
        }
        pool.publishUsage()
    }

    /**
     * Moves all in-memory items to flash — call before closing so unsent data survives a process
     * restart. Only writes when RAM is non-empty; a no-op when the pool is RAM-only.
     */
    fun flushToDisk() {
        val changed = synchronized(pool.lock) {
            if (ram.isEmpty() || !pool.spillToFlash) return
            val evicted = spillOldest(Long.MAX_VALUE) ?: return
            pool.publishUsage()
            pool.buffersOf(evicted).toSet() - this
        }
        notify(changed)
    }

    /** Leaves the pool. Anything still in RAM is discarded, so call [flushToDisk] first to keep it. */
    override fun close(): Unit = synchronized(pool.lock) {
        pool.ramChanged(-ramTierBytes)
        ram.clear()
        ramTierBytes = 0
        pool.closed(this)
        pool.publishUsage()
    }

    /**
     * Moves the oldest RAM items, at least [bytes] worth (or all), to flash in one transaction. Must hold
     * the pool lock. Returns the devices that lost data to flash eviction, or null if the write failed.
     */
    internal fun spillOldest(bytes: Long): Set<String>? {
        val batch = ArrayList<Item>()
        var batchBytes = 0L
        for (item in ram) {
            if (batchBytes >= bytes && batch.isNotEmpty()) break
            batch += item
            batchBytes += item.payload.size
        }
        val evicted = store.enqueueAll(deviceId, batch.map { MessageStore.Entry(it.seq, it.topic, it.payload) })
            ?: return null
        repeat(batch.size) { ram.removeFirst() }
        ramTierBytes -= batchBytes
        pool.ramChanged(-batchBytes)
        return evicted
    }

    /** Must hold the pool lock. */
    internal fun dropOldestRam() {
        val dropped = ram.removeFirst()
        ramTierBytes -= dropped.payload.size
        pool.ramChanged(-dropped.payload.size.toLong())
        Log.w(TAG, "buffer full; dropped oldest message of $deviceId (${dropped.payload.size} bytes)")
    }

    private fun notify(changed: Collection<StoreAndForwardBuffer>) {
        for (buffer in changed) if (buffer !== this) buffer.onExternalChange?.invoke()
    }

    private companion object {
        const val TAG = "SpotflowBuffer"
    }
}

package io.spotflow.ble.cloud

import io.spotflow.ble.Slog
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.io.Closeable

/**
 * How much of the gateway-wide store-and-forward budget is in use, across all devices. Sizes count each
 * message's payload plus a fixed estimate of the overhead of holding it.
 */
public data class BufferUsage(
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
 * data left on flash by an earlier run is delivered. The same happens, whatever the budget, while the
 * store is unusable (e.g. storage is full).
 *
 * All buffers share this pool's [lock]. [release] is called once, after [shutdown], when the last buffer
 * has closed.
 */
internal class BufferPool(
    private val store: MessageStore,
    private val ramMaxBytes: Long,
    private val flashMaxBytes: Long,
    /** What a message in RAM is counted as beyond its payload (object headers, queue slot). */
    internal val itemOverheadBytes: Long = ITEM_OVERHEAD_BYTES,
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

    /** False once the flash store turned out to be unusable (the pool then drops instead of spilling). */
    val storeAvailable: StateFlow<Boolean> get() = store.available

    /** See [MessageStore.handoffs]. */
    val handoffs: StateFlow<Int> get() = store.handoffs

    /** Must hold [lock]. Publishes the current [usage]. */
    internal fun publishUsage() {
        _usage.value = BufferUsage(ramUsed, ramMaxBytes, store.bytes, flashMaxBytes)
    }

    /**
     * Devices with data in flash, e.g. left behind when the app was killed during an outage. Opens the
     * store if it isn't yet, so call it from a background thread.
     */
    fun storedDeviceIds(): List<String> {
        val ids = store.storedDeviceIds()
        synchronized(lock) { publishUsage() } // the store's totals are known from here on
        return ids
    }

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
                changed += flashEvicted(evicted)
            }
        } else {
            dropUntilWithin(ramMaxBytes, changed)
        }
        return changed
    }

    /**
     * Must hold [lock]. Tells the buffers of [deviceIds] — devices that lost data to flash eviction — and
     * returns them.
     */
    internal fun flashEvicted(deviceIds: Set<String>): List<StoreAndForwardBuffer> {
        if (deviceIds.isEmpty()) return emptyList()
        return buffers.filter { it.deviceId in deviceIds }.onEach { it.onFlashEvicted() }
    }

    /** Must hold [lock]. */
    internal fun closed(buffer: StoreAndForwardBuffer) {
        buffers -= buffer
        // After a shutdown nothing here uploads what this buffer just left on flash: let a gateway that
        // is running (a restarted one overlaps with this one) know there is data to pick up.
        if (shutDown) store.handedOver()
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

    companion object {
        /**
         * What a message in RAM is counted as beyond its payload: the item and array headers, the topic
         * reference and its slot in the queue. An estimate that keeps the limit honest for tiny messages.
         */
        const val ITEM_OVERHEAD_BYTES = 64L
    }
}

/**
 * One device's store-and-forward buffer: a RAM queue in front of the device's part of the shared flash
 * [store], within the budgets of its [pool] (see [BufferPool] for when data moves to flash or is evicted).
 *
 * Every item gets a monotonic, per-device sequence number (from the [store]) that follows it across tiers,
 * so FIFO order is preserved (the flash tier always holds strictly lower sequence numbers than RAM and is
 * drained first) and a published item can be removed exactly, even if it was spilled to flash mid-publish.
 * Data still in RAM is lost if the process is killed — an accepted trade-off.
 *
 * Single-producer ([enqueue]) / single-consumer ([peek] then [remove]). [peek] does not take anything out,
 * so a failed publish needs no bookkeeping and there is neither reordering nor duplication.
 *
 * Flash is read and cleared in batches: the oldest rows are *staged* — read into memory with one query —
 * and deleted with one transaction once all of them have been published, rather than a query and a
 * transaction per message. If the process dies in between, the published part of a batch is uploaded
 * again on the next run (delivery is at-least-once anyway).
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

    /** The oldest flash rows not yet published, read ahead in one query. */
    private val staged = ArrayDeque<Item>()

    /** Staged rows that have been published; deleted together once the staged batch is through. */
    private val published = ArrayList<Long>()
    private var publishedBytes = 0L

    private var closed = false

    /** Must hold the pool lock. */
    internal var ramTierBytes = 0L
        private set

    /** Must hold the pool lock. */
    internal val ramCount: Int get() = ram.size

    /** Bytes of this device held in RAM. */
    val ramBytes: Long get() = synchronized(pool.lock) { ramTierBytes }

    /** Bytes of this device held in flash. */
    val diskBytes: Long get() = synchronized(pool.lock) { flashBytes() }

    /** Total bytes of this device across both tiers. */
    val bytes: Long get() = synchronized(pool.lock) { ramTierBytes + flashBytes() }

    /** Messages of this device waiting to be uploaded, across both tiers. */
    val count: Long get() = synchronized(pool.lock) { ram.size + flashRows() }

    /** Whether nothing of this device is buffered. */
    val isEmpty: Boolean get() = synchronized(pool.lock) { ram.isEmpty() && flashRows() == 0L }

    /**
     * Appends a message to RAM; the pool then spills or drops as needed to stay within its budget.
     * Returns the message's sequence number, or null if the buffer is closed (the message is dropped).
     */
    fun enqueue(topic: String, payload: ByteArray): Long? {
        val item: Item
        val changed = synchronized(pool.lock) {
            if (closed) {
                // Its owner is gone (the cloud link was closed): nothing would ever upload this.
                Slog.w(TAG, "dropping a message for $deviceId that arrived after its buffer was closed")
                return null
            }
            item = Item(store.nextSeq(deviceId), topic, payload)
            ram.addLast(item)
            ramTierBytes += cost(item)
            pool.ramChanged(cost(item))
            pool.enforceRamBudget().also { pool.publishUsage() }
        }
        notify(changed)
        return item.seq
    }

    /**
     * Returns the oldest items to publish, in order, without taking them out: up to [maxItems], stopping
     * early once their payloads exceed [maxPayloadBytes] (but always at least one, if any). Finalize each
     * with [remove] once it is published; a failed publish needs no action.
     *
     * Flash comes first, for FIFO: RAM only follows once nothing is left in flash beyond what is returned.
     * The flash check uses in-memory counters, so the common empty-flash path never queries SQLite.
     */
    fun peek(maxItems: Int = 1, maxPayloadBytes: Long = Long.MAX_VALUE): List<Item> = synchronized(pool.lock) {
        if (staged.isEmpty() && flashRows() > 0) stageFromFlash()
        val items = ArrayList<Item>(minOf(maxItems, staged.size + ram.size))
        var payloadBytes = 0L
        fun take(item: Item): Boolean {
            if (items.size >= maxItems) return false
            if (items.isNotEmpty() && payloadBytes + item.payload.size > maxPayloadBytes) return false
            items += item
            payloadBytes += item.payload.size
            return true
        }
        for (item in staged) if (!take(item)) return items
        if (flashRows() > staged.size) return items // older messages are still in flash, unstaged
        for (item in ram) if (!take(item)) return items
        items
    }

    /** The oldest item to publish, or null if empty; see [peek]. */
    fun takeNext(): Item? = peek(1).firstOrNull()

    /** Removes a published (or intentionally dropped) item, from whichever tier holds it. */
    fun remove(item: Item): Unit = synchronized(pool.lock) {
        val stagedIndex = staged.indexOfFirst { it.seq == item.seq }
        if (stagedIndex >= 0) {
            staged.removeAt(stagedIndex)
            published += item.seq
            publishedBytes += store.costOf(item.payload.size)
            if (staged.isEmpty()) commitPublished()
        } else {
            // Only the head of RAM is ever handed out, so the item is near the front if it is there at all.
            val ramIndex = ram.asSequence().take(RAM_SEARCH_DEPTH).indexOfFirst { it.seq == item.seq }
            if (ramIndex >= 0) {
                val removed = ram.removeAt(ramIndex)
                ramTierBytes -= cost(removed)
                pool.ramChanged(-cost(removed))
            } else {
                store.remove(deviceId, item.seq) // the item was spilled (possibly mid-publish) or evicted
            }
        }
        pool.publishUsage()
    }

    /**
     * Moves all in-memory items to flash — call before closing so unsent data survives a process
     * restart. Only writes when RAM is non-empty; a no-op when the pool is RAM-only.
     */
    fun flushToDisk() {
        val changed = synchronized(pool.lock) {
            commitPublished()
            if (ram.isEmpty() || !pool.spillToFlash) return
            val evicted = spillOldest(Long.MAX_VALUE) ?: return
            pool.publishUsage()
            pool.flashEvicted(evicted).toSet() - this
        }
        notify(changed)
    }

    /** Leaves the pool. Anything still in RAM is discarded, so call [flushToDisk] first to keep it. */
    override fun close(): Unit = synchronized(pool.lock) {
        if (closed) return
        closed = true
        commitPublished()
        staged.clear()
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
            batchBytes += cost(item)
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
        ramTierBytes -= cost(dropped)
        pool.ramChanged(-cost(dropped))
        Slog.w(TAG, "buffer full; dropped oldest message of $deviceId (${dropped.payload.size} bytes)")
    }

    /**
     * Must hold the pool lock. The store evicted some of this device's oldest rows — which are the staged
     * ones. Forget the read-ahead so the next [peek] starts from what flash holds now; an evicted item
     * that is being published at this moment still completes ([remove] finds nothing left to delete).
     */
    internal fun onFlashEvicted() {
        commitPublished()
        staged.clear()
    }

    private fun stageFromFlash() {
        store.peekBatch(deviceId, STAGE_ROWS, STAGE_PAYLOAD_BYTES).mapTo(staged) { Item(it.seq, it.topic, it.payload) }
    }

    /** Deletes the published part of the staged batch from flash, in one transaction. */
    private fun commitPublished() {
        if (published.isEmpty()) return
        store.removeAll(deviceId, published)
        published.clear()
        publishedBytes = 0
    }

    /** Rows of this device in flash that still have to be published. */
    private fun flashRows(): Long = (store.count(deviceId) - published.size).coerceAtLeast(0)

    private fun flashBytes(): Long = (store.bytes(deviceId) - publishedBytes).coerceAtLeast(0)

    private fun cost(item: Item): Long = item.payload.size + pool.itemOverheadBytes

    private fun notify(changed: Collection<StoreAndForwardBuffer>) {
        for (buffer in changed) if (buffer !== this) buffer.onExternalChange?.invoke()
    }

    private companion object {
        const val TAG = "SpotflowBuffer"

        /** How much of flash is read ahead, and so deleted, at a time. */
        const val STAGE_ROWS = 64
        const val STAGE_PAYLOAD_BYTES = 256L * 1024

        /** How far into RAM [remove] looks for an item; far more than is ever handed out at once. */
        const val RAM_SEARCH_DEPTH = 256
    }
}

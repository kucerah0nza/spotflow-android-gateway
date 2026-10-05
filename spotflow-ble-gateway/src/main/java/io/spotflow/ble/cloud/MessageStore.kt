package io.spotflow.ble.cloud

import android.content.ContentValues
import android.content.Context
import android.database.DatabaseUtils
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteException
import android.database.sqlite.SQLiteOpenHelper
import android.util.Log

/**
 * The flash tier of the store-and-forward buffer: one crash-safe SQLite database holding a FIFO of
 * pending MQTT publishes for every device, so buffered data survives the app being killed or the phone
 * rebooting during a network outage.
 *
 * Rows are keyed by device and an externally-assigned, per-device monotonic sequence number (supplied by
 * [StoreAndForwardBuffer]), so an item keeps its identity whether it lives in RAM or here. The total size
 * across all devices is bounded by [maxBytes]: when an append exceeds it, the oldest messages of the
 * device holding the most bytes are evicted first — so a device that floods the buffer, or stays offline
 * for long, gives up its own oldest data before anyone else's. The last remaining message is never
 * evicted, so a single message larger than [maxBytes] is kept rather than dropped.
 *
 * Writes are batched into one transaction per call (one flash sync per batch, not per message). Byte and
 * row counts are tracked per device in memory; a failed write (e.g. storage full) is logged and reported
 * to the caller instead of silently desynchronizing them. On first open, buffers left in the per-device
 * files of earlier versions are moved into the shared database.
 *
 * All operations are synchronized. Within a process the gateway shares one instance ([acquire]), so its
 * counters always match the database.
 */
internal class MessageStore(context: Context, maxBytes: Long) {

    /** A pending publish, identified within its device by its sequence number. */
    class Entry(val seq: Long, val topic: String, val payload: ByteArray)

    private val appContext = context.applicationContext

    /** Max total bytes across all devices. */
    @Volatile var maxBytes: Long = maxBytes

    private val helper = object : SQLiteOpenHelper(appContext, DB_NAME, null, SCHEMA_VERSION) {
        override fun onCreate(db: SQLiteDatabase) {
            db.execSQL(CREATE_QUEUE)
        }

        override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) = Unit
    }

    private val db: SQLiteDatabase = helper.writableDatabase

    /** Next sequence number per device, handed out by [nextSeq]. */
    private val nextSeqByDevice = HashMap<String, Long>()

    private val bytesByDevice = HashMap<String, Long>()
    private val rowsByDevice = HashMap<String, Long>()
    private var totalBytes = 0L
    private var totalRows = 0L

    init {
        migrateLegacyFiles()
        resync()
    }

    /** Bytes stored for all devices. */
    @get:Synchronized
    val bytes: Long get() = totalBytes

    /** Bytes stored for [deviceId]. */
    @Synchronized
    fun bytes(deviceId: String): Long = bytesByDevice[deviceId] ?: 0L

    /** Messages stored for [deviceId]. */
    @Synchronized
    fun count(deviceId: String): Long = rowsByDevice[deviceId] ?: 0L

    /** Whether nothing is stored for [deviceId] (row count, so zero-length payloads count). */
    @Synchronized
    fun isEmpty(deviceId: String): Boolean = (rowsByDevice[deviceId] ?: 0L) == 0L

    /** Devices with data stored, e.g. left behind when the app was killed during an outage. */
    @Synchronized
    fun storedDeviceIds(): List<String> = rowsByDevice.filterValues { it > 0 }.keys.toList()

    /**
     * Hands out the next sequence number for a message of [deviceId], after anything stored or handed out
     * before. Allocating here — in the one store the process shares — keeps numbers unique even while a
     * restarted gateway runs next to the previous one still flushing its RAM to flash.
     */
    @Synchronized
    fun nextSeq(deviceId: String): Long {
        val next = nextSeqByDevice[deviceId] ?: (maxSeq(deviceId) + 1)
        nextSeqByDevice[deviceId] = next + 1
        return next
    }

    /** The highest sequence number stored for [deviceId] (0 if none). */
    @Synchronized
    fun maxSeq(deviceId: String): Long =
        DatabaseUtils.longForQuery(db, "SELECT COALESCE(MAX(seq), 0) FROM q WHERE device = ?", arrayOf(deviceId))

    /**
     * Appends [entries] for [deviceId] in one transaction, then evicts as needed to stay within
     * [maxBytes]. Returns the devices that lost messages to eviction (possibly including [deviceId]), or
     * null — with none of the entries stored — if the write failed, e.g. because storage is full.
     */
    @Synchronized
    fun enqueueAll(deviceId: String, entries: List<Entry>): Set<String>? {
        if (entries.isEmpty()) return emptySet()
        var addedBytes = 0L
        var addedRows = 0L
        try {
            db.beginTransaction()
            try {
                for (entry in entries) {
                    // IGNORE: a duplicate (already stored) is not an error and must not be counted.
                    if (insert(deviceId, entry)) {
                        addedBytes += entry.payload.size
                        addedRows++
                    }
                }
                db.setTransactionSuccessful()
            } finally {
                db.endTransaction()
            }
        } catch (e: SQLiteException) {
            Log.w(TAG, "failed to persist ${entries.size} message(s) for $deviceId: ${e.message}")
            return null
        }
        adjust(deviceId, addedBytes, addedRows)
        return evictOverflow()
    }

    /** Returns the oldest message stored for [deviceId] without removing it, or null if none. */
    @Synchronized
    fun peek(deviceId: String): Entry? =
        db.rawQuery(
            "SELECT seq, topic, payload FROM q WHERE device = ? ORDER BY seq ASC LIMIT 1",
            arrayOf(deviceId),
        ).use { cursor ->
            if (!cursor.moveToFirst()) {
                // Self-heal: the table is the source of truth, so no rows means empty counters.
                resetCount(deviceId)
                return null
            }
            Entry(cursor.getLong(0), cursor.getString(1), cursor.getBlob(2))
        }

    /** Removes one message of [deviceId] by sequence number (no-op if it is not stored). */
    @Synchronized
    fun remove(deviceId: String, seq: Long) {
        try {
            if (!delete(deviceId, seq)) return
        } catch (e: SQLiteException) {
            Log.w(TAG, "failed to remove $deviceId/$seq: ${e.message}")
            resync()
        }
    }

    @Synchronized
    fun close() = helper.close()

    private fun insert(deviceId: String, entry: Entry): Boolean {
        val values = ContentValues().apply {
            put("device", deviceId)
            put("seq", entry.seq)
            put("topic", entry.topic)
            put("payload", entry.payload)
            put("len", entry.payload.size.toLong())
        }
        return db.insertWithOnConflict("q", null, values, SQLiteDatabase.CONFLICT_IGNORE) != -1L
    }

    /** Deletes one row and updates the counters; returns whether it existed. */
    private fun delete(deviceId: String, seq: Long): Boolean {
        val args = arrayOf(deviceId, seq.toString())
        val len = db.rawQuery("SELECT len FROM q WHERE device = ? AND seq = ?", args).use { cursor ->
            if (cursor.moveToFirst()) cursor.getLong(0) else return false
        }
        if (db.delete("q", "device = ? AND seq = ?", args) == 0) return false
        adjust(deviceId, -len, -1)
        return true
    }

    /** Evicts the oldest messages of the largest device until the total fits; returns who lost data. */
    private fun evictOverflow(): Set<String> {
        if (totalBytes <= maxBytes || totalRows <= 1) return emptySet()
        val evicted = HashMap<String, Int>()
        try {
            db.beginTransaction()
            try {
                while (totalBytes > maxBytes && totalRows > 1) {
                    val victim = bytesByDevice.maxByOrNull { it.value }?.key ?: break
                    val oldest = DatabaseUtils.longForQuery(
                        db, "SELECT MIN(seq) FROM q WHERE device = ?", arrayOf(victim),
                    )
                    if (!delete(victim, oldest)) {
                        resetCount(victim) // counters said it had data, the table says otherwise
                        continue
                    }
                    evicted.merge(victim, 1, Int::plus)
                }
                db.setTransactionSuccessful()
            } finally {
                db.endTransaction()
            }
        } catch (e: SQLiteException) {
            Log.w(TAG, "eviction failed: ${e.message}")
            resync()
        }
        if (evicted.isNotEmpty()) {
            Log.w(TAG, "buffer full (${maxBytes} bytes); evicted oldest messages: $evicted")
        }
        return evicted.keys
    }

    private fun adjust(deviceId: String, bytes: Long, rows: Long) {
        val newRows = (rowsByDevice[deviceId] ?: 0L) + rows
        if (newRows <= 0) {
            resetCount(deviceId)
            return
        }
        rowsByDevice[deviceId] = newRows
        bytesByDevice[deviceId] = ((bytesByDevice[deviceId] ?: 0L) + bytes).coerceAtLeast(0)
        totalBytes = (totalBytes + bytes).coerceAtLeast(0)
        totalRows = (totalRows + rows).coerceAtLeast(0)
    }

    private fun resetCount(deviceId: String) {
        totalBytes = (totalBytes - (bytesByDevice.remove(deviceId) ?: 0L)).coerceAtLeast(0)
        totalRows = (totalRows - (rowsByDevice.remove(deviceId) ?: 0L)).coerceAtLeast(0)
    }

    /** Recomputes the in-memory counters from the table (the source of truth). */
    private fun resync() {
        bytesByDevice.clear()
        rowsByDevice.clear()
        db.rawQuery("SELECT device, SUM(len), COUNT(*) FROM q GROUP BY device", null).use { c ->
            while (c.moveToNext()) {
                bytesByDevice[c.getString(0)] = c.getLong(1)
                rowsByDevice[c.getString(0)] = c.getLong(2)
            }
        }
        totalBytes = bytesByDevice.values.sum()
        totalRows = rowsByDevice.values.sum()
    }

    /**
     * Moves buffers from the per-device files of earlier versions (`spotflow_buffer_<device>.db`) into
     * this database, keeping their sequence numbers, then deletes each file. A file that can't be read
     * now is left in place and retried on the next start.
     */
    private fun migrateLegacyFiles() {
        val files = appContext.databaseList().filter { it.startsWith(LEGACY_PREFIX) && it.endsWith(".db") }
        for (name in files) {
            try {
                val moved = migrateLegacyFile(name)
                appContext.deleteDatabase(name)
                if (moved > 0) Log.i(TAG, "moved $moved buffered message(s) from $name")
            } catch (e: Exception) {
                Log.w(TAG, "cannot migrate $name yet: ${e.message}")
            }
        }
    }

    /** Returns how many messages were moved; throws if the file can't be read or written. */
    private fun migrateLegacyFile(name: String): Int {
        val path = appContext.getDatabasePath(name).path
        SQLiteDatabase.openDatabase(path, null, SQLiteDatabase.OPEN_READONLY).use { legacy ->
            val hasQueue = DatabaseUtils.longForQuery(
                legacy, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'q'", null,
            ) > 0
            if (!hasQueue) return 0
            val deviceId = legacyDeviceId(legacy, name)
            if (deviceId == null) {
                Log.w(TAG, "dropping $name: its device ID can't be recovered")
                return 0
            }
            var moved = 0
            db.beginTransaction()
            try {
                legacy.rawQuery("SELECT seq, topic, payload FROM q ORDER BY seq ASC", null).use { c ->
                    while (c.moveToNext()) {
                        if (insert(deviceId, Entry(c.getLong(0), c.getString(1), c.getBlob(2)))) moved++
                    }
                }
                db.setTransactionSuccessful()
            } finally {
                db.endTransaction()
            }
            return moved
        }
    }

    /** The device ID stored in a legacy file, or — for files from before it was stored — its name. */
    private fun legacyDeviceId(legacy: SQLiteDatabase, name: String): String? {
        val hasMeta = DatabaseUtils.longForQuery(
            legacy, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'meta'", null,
        ) > 0
        if (hasMeta) {
            legacy.rawQuery("SELECT v FROM meta WHERE k = 'device_id'", null).use { c ->
                if (c.moveToFirst()) return c.getString(0)
            }
        }
        // Without metadata the name is the ID, unless it was shortened/hashed (marked by '~').
        return name.removePrefix(LEGACY_PREFIX).removeSuffix(".db").takeUnless { it.contains('~') }
    }

    companion object {
        private const val TAG = "SpotflowBuffer"
        private const val DB_NAME = "spotflow_buffer.db"
        private const val LEGACY_PREFIX = "spotflow_buffer_"
        private const val SCHEMA_VERSION = 1
        private const val CREATE_QUEUE =
            "CREATE TABLE q (device TEXT NOT NULL, seq INTEGER NOT NULL, topic TEXT NOT NULL, " +
                "payload BLOB NOT NULL, len INTEGER NOT NULL, PRIMARY KEY (device, seq))"

        private var shared: MessageStore? = null
        private var users = 0

        /**
         * The process-wide store, opened on first use and kept open while any gateway uses it — a gateway
         * restarted by Stop/Start can overlap with the previous one still flushing its buffers. [maxBytes]
         * replaces the limit of earlier users. Balance with [release].
         */
        @Synchronized
        fun acquire(context: Context, maxBytes: Long): MessageStore {
            val store = shared ?: MessageStore(context, maxBytes).also { shared = it }
            store.maxBytes = maxBytes
            users++
            return store
        }

        @Synchronized
        fun release(store: MessageStore) {
            if (store !== shared || --users > 0) return
            store.close()
            shared = null
        }
    }
}

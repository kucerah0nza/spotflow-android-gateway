package io.spotflow.ble.cloud

import android.content.ContentValues
import android.content.Context
import android.database.DatabaseUtils
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteException
import android.database.sqlite.SQLiteOpenHelper
import android.util.Log
import java.io.Closeable
import java.security.MessageDigest

/**
 * A crash-safe, byte-bounded FIFO of pending MQTT publishes, persisted to a per-device SQLite database
 * so buffered data survives the app being killed or the phone rebooting during a network outage.
 *
 * Rows are keyed by an externally-assigned monotonic [Entry.seq] (supplied by [StoreAndForwardBuffer]),
 * so an item keeps the same identity whether it lives in the RAM tier or is spilled here — which lets the
 * buffer remove exactly the item it published, and preserves global FIFO order. When appending would
 * exceed [maxBytes], the oldest messages are evicted (a circular buffer); a single message larger than
 * [maxBytes] is kept alone rather than dropped.
 *
 * Writes are batched into one transaction per call (one flash sync per batch, not per message). Byte and
 * row counts are tracked in memory, and a failed write (e.g. storage full) is logged and reported to the
 * caller instead of silently desynchronizing those counters. The device ID is stored alongside the data
 * so [storedDeviceIds] can find and drain leftover buffers after a restart.
 *
 * All operations are synchronized; the slow part (the actual MQTT publish) happens outside this class.
 */
class PersistentMessageQueue(
    context: Context,
    deviceId: String,
    private val maxBytes: Long,
) : Closeable {

    /** A pending publish, identified by its buffer-wide sequence number. */
    class Entry(val seq: Long, val topic: String, val payload: ByteArray)

    private val appContext = context.applicationContext
    private val dbName = databaseName(deviceId)

    private val helper = object : SQLiteOpenHelper(appContext, dbName, null, SCHEMA_VERSION) {
        override fun onCreate(db: SQLiteDatabase) {
            db.execSQL(CREATE_QUEUE)
            db.execSQL(CREATE_META)
        }

        override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
            // v1 had an incompatible layout; from v2 on the queue is kept so buffered data survives.
            if (oldVersion < 2) {
                db.execSQL("DROP TABLE IF EXISTS q")
                db.execSQL(CREATE_QUEUE)
            }
            if (oldVersion < 3) db.execSQL(CREATE_META)
        }
    }

    private val db: SQLiteDatabase = helper.writableDatabase.also {
        it.execSQL("INSERT OR REPLACE INTO meta (k, v) VALUES ('$META_DEVICE_ID', ?)", arrayOf(deviceId))
    }

    private var totalBytes = 0L
    private var rowCount = 0L

    init {
        resync()
    }

    /** Current buffered size in bytes. */
    @get:Synchronized
    val bytes: Long get() = totalBytes

    /** Whether nothing is stored (independent of payload sizes, so zero-length payloads count). */
    @get:Synchronized
    val isEmpty: Boolean get() = rowCount == 0L

    /** The highest sequence number currently stored (0 if empty), so the buffer can resume after reopen. */
    @Synchronized
    fun maxSeq(): Long = DatabaseUtils.longForQuery(db, "SELECT COALESCE(MAX(seq), 0) FROM q", null)

    /** Appends one message; see [enqueueAll]. */
    fun enqueue(seq: Long, topic: String, payload: ByteArray): Boolean =
        enqueueAll(listOf(Entry(seq, topic, payload)))

    /**
     * Appends [entries] in one transaction, then evicts the oldest as needed to stay within [maxBytes].
     * Returns false (and stores none of them) if the write failed, e.g. because storage is full.
     */
    @Synchronized
    fun enqueueAll(entries: List<Entry>): Boolean {
        if (entries.isEmpty()) return true
        var addedBytes = 0L
        var addedRows = 0L
        try {
            db.beginTransaction()
            try {
                for (entry in entries) {
                    val values = ContentValues().apply {
                        put("seq", entry.seq)
                        put("topic", entry.topic)
                        put("payload", entry.payload)
                        put("len", entry.payload.size.toLong())
                    }
                    // IGNORE: a duplicate seq (already stored) is not an error and must not be counted.
                    if (db.insertWithOnConflict("q", null, values, SQLiteDatabase.CONFLICT_IGNORE) != -1L) {
                        addedBytes += entry.payload.size
                        addedRows++
                    }
                }
                db.setTransactionSuccessful()
            } finally {
                db.endTransaction()
            }
        } catch (e: SQLiteException) {
            Log.w(TAG, "failed to persist ${entries.size} message(s): ${e.message}")
            return false
        }
        totalBytes += addedBytes
        rowCount += addedRows
        evictOverflow()
        return true
    }

    /** Returns the oldest pending message without removing it, or null if empty. */
    @Synchronized
    fun peek(): Entry? =
        db.rawQuery("SELECT seq, topic, payload FROM q ORDER BY seq ASC LIMIT 1", null).use { cursor ->
            if (!cursor.moveToFirst()) {
                // Self-heal: the table is the source of truth, so an empty table means empty counters.
                totalBytes = 0
                rowCount = 0
                return null
            }
            Entry(cursor.getLong(0), cursor.getString(1), cursor.getBlob(2))
        }

    /** Removes a message by sequence number (no-op if it is not on disk). */
    @Synchronized
    fun remove(seq: Long) {
        try {
            val len = db.rawQuery("SELECT len FROM q WHERE seq = ?", arrayOf(seq.toString())).use { cursor ->
                if (cursor.moveToFirst()) cursor.getLong(0) else return
            }
            if (db.delete("q", "seq = ?", arrayOf(seq.toString())) > 0) {
                totalBytes = (totalBytes - len).coerceAtLeast(0)
                rowCount = (rowCount - 1).coerceAtLeast(0)
            }
        } catch (e: SQLiteException) {
            Log.w(TAG, "failed to remove seq $seq: ${e.message}")
            resync()
        }
    }

    /** Closes the database; if [deleteIfEmpty] and nothing is buffered, also deletes its file. */
    @Synchronized
    fun close(deleteIfEmpty: Boolean) {
        val empty = runCatching { resync(); rowCount == 0L }.getOrDefault(false)
        helper.close()
        if (deleteIfEmpty && empty) appContext.deleteDatabase(dbName)
    }

    @Synchronized
    override fun close() = close(deleteIfEmpty = false)

    private fun evictOverflow() {
        if (totalBytes <= maxBytes || rowCount <= 1) return
        try {
            db.beginTransaction()
            try {
                while (totalBytes > maxBytes && rowCount > 1) {
                    val evicted = db.rawQuery("SELECT seq, len FROM q ORDER BY seq ASC LIMIT 1", null).use { c ->
                        if (c.moveToFirst()) c.getLong(0) to c.getLong(1) else null
                    } ?: break
                    db.delete("q", "seq = ?", arrayOf(evicted.first.toString()))
                    totalBytes -= evicted.second
                    rowCount--
                }
                db.setTransactionSuccessful()
            } finally {
                db.endTransaction()
            }
        } catch (e: SQLiteException) {
            Log.w(TAG, "eviction failed: ${e.message}")
            resync()
        }
    }

    /** Recomputes the in-memory counters from the table (the source of truth). */
    private fun resync() {
        db.rawQuery("SELECT COALESCE(SUM(len), 0), COUNT(*) FROM q", null).use { c ->
            c.moveToFirst()
            totalBytes = c.getLong(0)
            rowCount = c.getLong(1)
        }
    }

    companion object {
        private const val TAG = "SpotflowBuffer"
        private const val SCHEMA_VERSION = 3
        private const val FILE_PREFIX = "spotflow_buffer_"
        private const val META_DEVICE_ID = "device_id"
        private const val CREATE_QUEUE =
            "CREATE TABLE q (seq INTEGER PRIMARY KEY, topic TEXT NOT NULL, payload BLOB NOT NULL, len INTEGER NOT NULL)"
        private const val CREATE_META = "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)"

        /**
         * The database file name for [deviceId]. IDs that are already file-name safe keep the plain
         * (backward-compatible) name; anything else gets a hash suffix so distinct IDs never collide.
         */
        internal fun databaseName(deviceId: String): String {
            val sanitized = deviceId.map { if (it.isLetterOrDigit() || it == '-' || it == '_') it else '_' }
                .joinToString("")
            val lossless = sanitized == deviceId && deviceId.length <= 64
            val name = if (lossless) deviceId else "${sanitized.take(40)}~${sha256(deviceId).take(16)}"
            return "$FILE_PREFIX$name.db"
        }

        /**
         * Device IDs that have a non-empty buffer on disk (e.g. left behind when the app was killed during
         * an outage), so the gateway can drain them without waiting for the device to reconnect.
         */
        fun storedDeviceIds(context: Context): List<String> =
            context.applicationContext.databaseList()
                .filter { it.startsWith(FILE_PREFIX) && it.endsWith(".db") }
                .mapNotNull { name ->
                    runCatching {
                        val path = context.applicationContext.getDatabasePath(name).path
                        SQLiteDatabase.openDatabase(path, null, SQLiteDatabase.OPEN_READONLY).use { db ->
                            val hasData = DatabaseUtils.longForQuery(db, "SELECT EXISTS (SELECT 1 FROM q)", null) == 1L
                            val id = DatabaseUtils.stringForQuery(
                                db, "SELECT v FROM meta WHERE k = '$META_DEVICE_ID'", null,
                            )
                            id.takeIf { hasData }
                        }
                    }.getOrNull() // pre-v3 file (no meta) or unreadable: drained when the device reconnects
                }

        private fun sha256(s: String): String =
            MessageDigest.getInstance("SHA-256").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }
    }
}

package io.spotflow.ble.cloud

import android.content.ContentValues
import android.content.Context
import android.database.DatabaseUtils
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import java.io.Closeable

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
 * All operations are synchronized; the slow part (the actual MQTT publish) happens outside this class.
 */
class PersistentMessageQueue(
    context: Context,
    deviceId: String,
    private val maxBytes: Long,
) : Closeable {

    /** A pending publish, identified by its buffer-wide sequence number. */
    class Entry(val seq: Long, val topic: String, val payload: ByteArray)

    private val helper = object : SQLiteOpenHelper(
        context.applicationContext,
        "spotflow_buffer_${sanitize(deviceId)}.db",
        null,
        2,
    ) {
        override fun onCreate(db: SQLiteDatabase) {
            db.execSQL(
                "CREATE TABLE q (seq INTEGER PRIMARY KEY, topic TEXT NOT NULL, " +
                    "payload BLOB NOT NULL, len INTEGER NOT NULL)",
            )
        }

        override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
            db.execSQL("DROP TABLE IF EXISTS q")
            onCreate(db)
        }
    }

    private val db: SQLiteDatabase = helper.writableDatabase

    private var totalBytes: Long =
        DatabaseUtils.longForQuery(db, "SELECT COALESCE(SUM(len), 0) FROM q", null)

    /** Current buffered size in bytes. */
    @get:Synchronized
    val bytes: Long get() = totalBytes

    /** The highest sequence number currently stored (0 if empty), so the buffer can resume after reopen. */
    @Synchronized
    fun maxSeq(): Long = DatabaseUtils.longForQuery(db, "SELECT COALESCE(MAX(seq), 0) FROM q", null)

    /** Appends a message with its [seq], evicting the oldest as needed to stay within [maxBytes]. */
    @Synchronized
    fun enqueue(seq: Long, topic: String, payload: ByteArray) {
        val values = ContentValues().apply {
            put("seq", seq)
            put("topic", topic)
            put("payload", payload)
            put("len", payload.size.toLong())
        }
        db.insert("q", null, values)
        totalBytes += payload.size

        while (totalBytes > maxBytes && DatabaseUtils.queryNumEntries(db, "q") > 1) {
            db.rawQuery("SELECT seq, len FROM q ORDER BY seq ASC LIMIT 1", null).use { cursor ->
                if (cursor.moveToFirst()) {
                    val oldestSeq = cursor.getLong(0)
                    val oldestLen = cursor.getLong(1)
                    db.delete("q", "seq = ?", arrayOf(oldestSeq.toString()))
                    totalBytes -= oldestLen
                }
            }
        }
    }

    /** Returns the oldest pending message without removing it, or null if empty. */
    @Synchronized
    fun peek(): Entry? =
        db.rawQuery("SELECT seq, topic, payload FROM q ORDER BY seq ASC LIMIT 1", null).use { cursor ->
            if (!cursor.moveToFirst()) return null
            Entry(cursor.getLong(0), cursor.getString(1), cursor.getBlob(2))
        }

    /** Removes a message by sequence number (no-op if it is not on disk). */
    @Synchronized
    fun remove(seq: Long) {
        db.rawQuery("SELECT len FROM q WHERE seq = ?", arrayOf(seq.toString())).use { cursor ->
            if (cursor.moveToFirst()) {
                totalBytes -= cursor.getLong(0)
                if (totalBytes < 0) totalBytes = 0
            }
        }
        db.delete("q", "seq = ?", arrayOf(seq.toString()))
    }

    @Synchronized
    override fun close() = helper.close()

    private companion object {
        fun sanitize(name: String): String =
            name.map { if (it.isLetterOrDigit() || it == '-' || it == '_') it else '_' }.joinToString("")
                .take(64)
    }
}

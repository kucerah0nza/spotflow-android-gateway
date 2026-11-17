package io.spotflow.ble

import android.util.Log

/** Receives the library's log output; see [SpotflowLog]. */
public fun interface SpotflowLogger {
    /** [priority] is one of the `android.util.Log` priorities (`Log.DEBUG` … `Log.ERROR`). */
    public fun log(priority: Int, tag: String, message: String, error: Throwable?)
}

/**
 * Where the library logs. By default it writes to logcat at `INFO` and above; log lines can contain
 * device IDs, so a host app may want to route them through its own logging, raise [minPriority], or set
 * [logger] to `null` to silence the library.
 */
public object SpotflowLog {

    /** Receives every message at or above [minPriority]; `null` drops them all. */
    @Volatile
    public var logger: SpotflowLogger? = SpotflowLogger { priority, tag, message, error ->
        Log.println(priority, tag, if (error == null) message else "$message\n${Log.getStackTraceString(error)}")
    }

    /** Messages below this `android.util.Log` priority are dropped. */
    @Volatile
    public var minPriority: Int = Log.INFO
}

/** The library's internal logging entry point. */
internal object Slog {
    fun d(tag: String, message: String) = emit(Log.DEBUG, tag, message, null)
    fun i(tag: String, message: String) = emit(Log.INFO, tag, message, null)
    fun w(tag: String, message: String, error: Throwable? = null) = emit(Log.WARN, tag, message, error)
    fun e(tag: String, message: String, error: Throwable? = null) = emit(Log.ERROR, tag, message, error)

    private fun emit(priority: Int, tag: String, message: String, error: Throwable?) {
        if (priority < SpotflowLog.minPriority) return
        val logger = SpotflowLog.logger ?: return
        // A host-supplied logger must never be able to break relaying.
        runCatching { logger.log(priority, tag, message, error) }
    }
}

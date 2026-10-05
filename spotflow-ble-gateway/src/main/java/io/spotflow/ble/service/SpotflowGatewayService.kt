package io.spotflow.ble.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import io.spotflow.ble.SpotflowGateway

/**
 * A foreground service that hosts a [SpotflowGateway] so relaying continues while the app is backgrounded
 * or the screen is off. It runs with foreground service type `connectedDevice` and a persistent
 * notification, as Android requires for long-lived BLE work.
 *
 * The host app configures it before starting:
 * ```
 * SpotflowGatewayService.gatewayFactory = { ctx -> SpotflowGateway(ctx, StaticIngestKey(key)) }
 * SpotflowGatewayService.onReady = { gateway -> gateway.startScanning() }
 * SpotflowGatewayService.start(context)
 * ```
 * The service is `START_STICKY`: if Android kills the process, it recreates the service later — in a
 * fresh process where these static hooks are unset. To keep relaying across such restarts, set
 * [gatewayFactory] and [onReady] from `Application.onCreate()` (which runs first in the new process)
 * whenever the gateway should be running; otherwise the restarted service just stops itself.
 *
 * Host apps that already run their own foreground service (e.g. in attach mode) can skip this class and
 * hold a [SpotflowGateway] directly.
 */
class SpotflowGatewayService : Service() {

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        // Enter foreground first to satisfy the startForegroundService contract before any early return.
        try {
            startForegroundCompat(notificationProvider?.invoke(this) ?: buildDefaultNotification())
        } catch (e: RuntimeException) {
            // e.g. Android 14+ refuses a connectedDevice service once Bluetooth permissions were revoked
            // (possible on a START_STICKY restart). Stop instead of crashing the host app.
            Log.w(TAG, "cannot enter foreground: ${e.message}; stopping self")
            stopSelf()
            return
        }

        val factory = gatewayFactory
        if (factory == null) {
            // The process was likely killed and restarted by the system (START_STICKY): the static
            // factory is gone. Stop gracefully rather than crashing; the host re-creates it on next start.
            Log.w(TAG, "gatewayFactory not set (process restarted?); stopping self")
            stopSelf()
            return
        }

        val instance = factory(this)
        gateway = instance
        onReady?.invoke(instance)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_RESTART) restartGateway()
        return START_STICKY
    }

    /**
     * Replaces the gateway with a fresh one from [gatewayFactory] — e.g. to apply new settings — while the
     * service stays in the foreground. Devices in range reconnect within seconds, and data the old gateway
     * still held is flushed to flash and picked up by the new one, so relaying pauses only briefly.
     */
    private fun restartGateway() {
        val factory = gatewayFactory ?: return
        Log.i(TAG, "restarting the gateway")
        gateway?.shutdown()
        gateway = null
        val instance = factory(this)
        gateway = instance
        onReady?.invoke(instance)
    }

    override fun onDestroy() {
        gateway?.shutdown()
        gateway = null
        super.onDestroy()
    }

    private fun startForegroundCompat(notification: Notification) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE)
        } else {
            startForeground(NOTIFICATION_ID, notification)
        }
    }

    private fun buildDefaultNotification(): Notification {
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        val channel = NotificationChannel(
            CHANNEL_ID,
            "Spotflow gateway",
            NotificationManager.IMPORTANCE_LOW,
        ).apply { description = "Relays device diagnostics to Spotflow" }
        manager.createNotificationChannel(channel)

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("Spotflow gateway active")
            .setContentText("Relaying device diagnostics")
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .setOngoing(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    companion object {
        private const val TAG = "SpotflowGateway"
        const val CHANNEL_ID = "spotflow_gateway"
        private const val NOTIFICATION_ID = 4711
        private const val ACTION_RESTART = "io.spotflow.ble.action.RESTART_GATEWAY"

        /** Builds the [SpotflowGateway] the service will host. Required. */
        @Volatile
        var gatewayFactory: ((Context) -> SpotflowGateway)? = null

        /**
         * Optional custom foreground notification (branding). Falls back to a default. The provider must
         * create its own notification channel.
         */
        @Volatile
        var notificationProvider: ((Context) -> Notification)? = null

        /** Called once the gateway is created; typically calls `gateway.startScanning()`. */
        @Volatile
        var onReady: ((SpotflowGateway) -> Unit)? = null

        /** The live gateway instance while the service runs, for observing [SpotflowGateway.devices]. */
        @Volatile
        var gateway: SpotflowGateway? = null
            private set

        fun start(context: Context) {
            ContextCompat.startForegroundService(context, Intent(context, SpotflowGatewayService::class.java))
        }

        /**
         * Restarts a running gateway in place (see [gatewayFactory]), much faster than [stop] + [start]
         * since the service never leaves the foreground. Starts it if it isn't running. Call it while the
         * app is in the foreground, as Android limits starting services from the background.
         */
        fun restart(context: Context) {
            if (gateway == null) {
                start(context)
            } else {
                context.startService(Intent(context, SpotflowGatewayService::class.java).setAction(ACTION_RESTART))
            }
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, SpotflowGatewayService::class.java))
        }
    }
}

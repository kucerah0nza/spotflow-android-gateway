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
 * Host apps that already run their own foreground service (e.g. in attach mode) can skip this class and
 * hold a [SpotflowGateway] directly.
 */
class SpotflowGatewayService : Service() {

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        // Enter foreground first to satisfy the startForegroundService contract before any early return.
        startForegroundCompat(notificationProvider?.invoke(this) ?: buildDefaultNotification())

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

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int = START_STICKY

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

        /** Builds the [SpotflowGateway] the service will host. Required. */
        @Volatile
        var gatewayFactory: ((Context) -> SpotflowGateway)? = null

        /** Optional custom foreground notification (branding). Falls back to a default. */
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

        fun stop(context: Context) {
            context.stopService(Intent(context, SpotflowGatewayService::class.java))
        }
    }
}

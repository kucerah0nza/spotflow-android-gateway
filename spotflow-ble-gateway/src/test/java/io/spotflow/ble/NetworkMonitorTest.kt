package io.spotflow.ble

import android.content.Context
import android.net.ConnectivityManager
import androidx.test.core.app.ApplicationProvider
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.Shadows.shadowOf

@RunWith(RobolectricTestRunner::class)
class NetworkMonitorTest {

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val connectivity = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager

    @Test
    fun `follows the default network coming and going`() {
        val monitor = NetworkMonitor(context)
        monitor.start()
        assertTrue(monitor.online.value)
        val callback = shadowOf(connectivity).networkCallbacks.single()
        val network = connectivity.activeNetwork!!

        callback.onLost(network)
        assertFalse(monitor.online.value)

        callback.onAvailable(network)
        assertTrue(monitor.online.value)

        monitor.stop()
        assertTrue("the callback is unregistered", shadowOf(connectivity).networkCallbacks.isEmpty())
    }

    @Test
    fun `starts out offline when there is no network to begin with`() {
        shadowOf(connectivity).setActiveNetworkInfo(null)
        val monitor = NetworkMonitor(context)

        monitor.start()

        assertFalse("nothing announces a network that isn't there", monitor.online.value)
        monitor.stop()
    }

    @Test
    fun `assumes a network until it is started`() {
        assertTrue(NetworkMonitor(context).online.value)
    }
}

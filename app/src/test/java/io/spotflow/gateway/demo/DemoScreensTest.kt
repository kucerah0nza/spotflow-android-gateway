package io.spotflow.gateway.demo

import android.bluetooth.BluetoothManager
import android.content.Context
import android.os.Looper
import android.view.View
import android.widget.TextView
import androidx.test.core.app.ActivityScenario
import androidx.test.core.app.ApplicationProvider
import com.google.android.material.textfield.TextInputLayout
import io.spotflow.ble.DeviceFilter
import io.spotflow.ble.ScanState
import io.spotflow.ble.SpotflowGateway
import io.spotflow.ble.cloud.StaticIngestKey
import io.spotflow.ble.service.SpotflowGatewayService
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Robolectric
import org.robolectric.RobolectricTestRunner
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config

/**
 * Smoke tests of the demo's screens: that they come up, and show what the gateway reports. The gateway
 * here is the real one, hosted by the real service; only Android is simulated.
 */
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35])
class DemoScreensTest {

    private val context = ApplicationProvider.getApplicationContext<Context>()

    @After
    fun tearDown() {
        SpotflowGatewayService.gatewayFactory = null
        SpotflowGatewayService.onReady = null
    }

    private fun idle() = shadowOf(Looper.getMainLooper()).idle()

    @Test
    fun `the main screen starts idle and points to the settings`() {
        ActivityScenario.launch(MainActivity::class.java).use { scenario ->
            scenario.onActivity { activity ->
                idle()
                assertEquals(
                    context.getString(R.string.idle_no_key),
                    activity.findViewById<TextView>(R.id.status).text.toString(),
                )
                assertTrue(activity.findViewById<View>(R.id.startButton).isEnabled)
                assertFalse(activity.findViewById<View>(R.id.stopButton).isEnabled)
                assertFalse(activity.findViewById<View>(R.id.errorBanner).isShown)
            }
        }
    }

    @Test
    fun `while the gateway runs, the main screen follows it and says what discovery is doing`() {
        val bluetooth = (context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager).adapter
        @Suppress("DEPRECATION") // the shadow's own switch for "Bluetooth is off"
        shadowOf(bluetooth).setEnabled(false)
        SpotflowGatewayService.gatewayFactory = { ctx -> SpotflowGateway(ctx, StaticIngestKey("key"), DeviceFilter.AcceptAll) }
        SpotflowGatewayService.onReady = { it.startScanning() }
        val service = Robolectric.buildService(SpotflowGatewayService::class.java).create()
        try {
            val gateway = SpotflowGatewayService.gateway
            assertNotNull("the service hosts a gateway", gateway)
            assertEquals(ScanState.WAITING_FOR_BLUETOOTH, gateway!!.status.value.scan)

            ActivityScenario.launch(MainActivity::class.java).use { scenario ->
                scenario.onActivity { activity ->
                    idle()
                    assertEquals(
                        context.getString(R.string.status_bluetooth_off),
                        activity.findViewById<TextView>(R.id.status).text.toString(),
                    )
                    assertFalse("running: Start is off", activity.findViewById<View>(R.id.startButton).isEnabled)
                    assertTrue(activity.findViewById<View>(R.id.stopButton).isEnabled)
                }
            }
        } finally {
            service.destroy()
        }
        assertNull("stopping the service ends the gateway", SpotflowGatewayService.gateway)
        assertNull(SpotflowGatewayService.gatewayFlow.value)
    }

    @Test
    fun `the settings screen rejects a missing key and absurd buffer sizes`() {
        ActivityScenario.launch(SettingsActivity::class.java).use { scenario ->
            scenario.onActivity { activity ->
                activity.findViewById<TextView>(R.id.bufferRamMb).text = "100000"
                activity.findViewById<TextView>(R.id.bufferFlashMb).text = "12"
                activity.findViewById<View>(R.id.saveButton).performClick()
                idle()

                assertEquals(
                    context.getString(R.string.ingest_key_required),
                    activity.findViewById<TextInputLayout>(R.id.ingestKeyLayout).error?.toString(),
                )
                assertNotNull(activity.findViewById<TextInputLayout>(R.id.bufferRamLayout).error)
                assertNull(activity.findViewById<TextInputLayout>(R.id.bufferFlashLayout).error)
                assertFalse("nothing was saved", activity.isFinishing)
            }
        }
    }

    @Test
    fun `saved settings count as unapplied until a gateway is built from them`() {
        val store = IngestKeyStore(context)
        assertFalse(store.hasUnappliedSettings)

        store.save(ingestKey = "", bufferRamMb = 2, bufferFlashMb = 60)
        assertTrue(store.hasUnappliedSettings)
        val version = store.settingsVersion

        store.save(ingestKey = "", bufferRamMb = 2, bufferFlashMb = 60) // saved again, unchanged
        assertEquals("nothing changed, so nothing to apply", version, store.settingsVersion)

        store.appliedSettingsVersion = store.settingsVersion
        assertFalse(store.hasUnappliedSettings)
    }

    @Test
    fun `nothing derived from the ingest key is kept in plain preferences`() {
        // What versions up to 0.1.8 stored to tell whether settings had changed: a hash of the key.
        context.getSharedPreferences("spotflow_prefs", Context.MODE_PRIVATE)
            .edit().putString("applied_settings", "5e884898da28…|1|50").commit()

        IngestKeyStore(context)
        idle()

        val prefs = context.getSharedPreferences("spotflow_prefs", Context.MODE_PRIVATE)
        assertFalse(prefs.contains("applied_settings"))
    }

    @Test
    fun `the messages with placeholders format`() {
        assertEquals("⚠ bad key", context.getString(R.string.problem, "bad key"))
        assertEquals("⚠ bad key\nfix it", context.getString(R.string.problem_with_hint, "bad key", "fix it"))
        assertTrue(context.getString(R.string.status_scan_retrying, "error 2").contains("error 2"))
        assertTrue(context.getString(R.string.buffer_summary_offline, "Buffer").startsWith("Buffer"))
    }
}

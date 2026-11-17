package io.spotflow.ble

import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.os.Build
import androidx.annotation.RequiresApi
import io.spotflow.ble.transport.AttachedBleConnection
import io.spotflow.ble.transport.SpotflowGattSession
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Relays a device over a `BluetoothGatt` that the host app owns — for apps that already hold a
 * connection to the device and don't want a second one. Create it with
 * [SpotflowGateway.newAttachment].
 *
 * It works in two steps, so that the gateway never issues a GATT operation before its callbacks can
 * reach it:
 *
 *  1. **Wire [gattCallback]** into the GATT's callbacks. Either pass it straight to `connectGatt`, or —
 *     if the app has a callback of its own — forward *every* event that callback receives to it (both
 *     the API 33+ and the older overloads of `onCharacteristicRead` / `onCharacteristicChanged`).
 *  2. **Call [start]** with the GATT. It may still be connecting; the gateway waits for the link.
 *
 * While attached, run the app's own GATT operations inside [runExclusive]: Android allows only one
 * outstanding GATT operation at a time, and this is what keeps the app's and the gateway's apart.
 *
 * The host keeps ownership of the link: it connects, disconnects and reconnects. A relaying session ends
 * when the link drops; call [start] again once it is back (the same [gattCallback] stays valid, so a GATT
 * reconnected with `BluetoothGatt.connect()` needs no rewiring). [detach] stops relaying and tells the
 * device to stop streaming, leaving the link up.
 */
public class GattAttachment internal constructor(
    private val gateway: SpotflowGateway,
    private val requestedMtu: Int,
    private val pollRssi: Boolean,
    private val isLinkUp: (BluetoothGatt) -> Boolean,
) {
    private class Active(val gatt: BluetoothGatt, val session: SpotflowGattSession, val connection: AttachedBleConnection)

    /** The session [start] created last; what [runExclusive] and [detach] act on. */
    @Volatile private var active: Active? = null

    /**
     * Every session that may still be waiting for a callback: the current one, and a previous one for as
     * long as it is winding down (it turns the device's stream off, and needs the answer to that).
     */
    private val sessions = CopyOnWriteArrayList<Active>()

    /**
     * Hands an event to the sessions of the GATT it came from. With two GATTs on one attachment, each
     * session only ever sees its own device's events.
     */
    private inline fun forward(gatt: BluetoothGatt, deliver: (BluetoothGattCallback) -> Unit) {
        for (entry in sessions) {
            if (entry.session.isClosed) sessions.remove(entry) else if (entry.gatt === gatt) deliver(entry.session.callback)
        }
    }

    /**
     * Receives the GATT's events for the gateway. Valid for the life of this attachment, across [start]s.
     */
    public val gattCallback: BluetoothGattCallback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
            forward(gatt) { it.onConnectionStateChange(gatt, status, newState) }
        }

        override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
            forward(gatt) { it.onServicesDiscovered(gatt, status) }
        }

        override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
            forward(gatt) { it.onMtuChanged(gatt, mtu, status) }
        }

        // Only called by Android 13+; older versions call the overload below.
        @RequiresApi(Build.VERSION_CODES.TIRAMISU)
        override fun onCharacteristicRead(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            value: ByteArray,
            status: Int,
        ) {
            forward(gatt) { it.onCharacteristicRead(gatt, characteristic, value, status) }
        }

        @Deprecated("Deprecated in API 33")
        @Suppress("DEPRECATION")
        override fun onCharacteristicRead(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            status: Int,
        ) {
            forward(gatt) { it.onCharacteristicRead(gatt, characteristic, status) }
        }

        override fun onCharacteristicWrite(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            status: Int,
        ) {
            forward(gatt) { it.onCharacteristicWrite(gatt, characteristic, status) }
        }

        override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
            forward(gatt) { it.onDescriptorWrite(gatt, descriptor, status) }
        }

        override fun onReadRemoteRssi(gatt: BluetoothGatt, rssi: Int, status: Int) {
            forward(gatt) { it.onReadRemoteRssi(gatt, rssi, status) }
        }

        // Only called by Android 13+; older versions call the overload below.
        @RequiresApi(Build.VERSION_CODES.TIRAMISU)
        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            value: ByteArray,
        ) {
            forward(gatt) { it.onCharacteristicChanged(gatt, characteristic, value) }
        }

        @Deprecated("Deprecated in API 33")
        @Suppress("DEPRECATION")
        override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
            forward(gatt) { it.onCharacteristicChanged(gatt, characteristic) }
        }
    }

    /**
     * Starts relaying the device behind [gatt]. Call it once [gattCallback] receives the GATT's events.
     * If the link isn't up yet the gateway waits for it (up to 30 s). Calling it again — after the link
     * dropped and came back, or with another GATT — replaces the previous session: an attachment relays
     * one device at a time (use one attachment per device to relay several).
     */
    public fun start(gatt: BluetoothGatt) {
        val previous = active
        val session = SpotflowGattSession(requestedMtu)
        val connection = AttachedBleConnection(gatt, session) { isLinkUp(gatt) }
        val current = Active(gatt, session, connection)
        sessions += current
        active = current
        // Another device: its session is not replaced by the new one's address, so end it here.
        if (previous != null && previous.gatt.device.address != gatt.device.address) {
            gateway.detach(previous.gatt.device.address)
        }
        gateway.startAttached(connection, pollRssi)
    }

    /**
     * Runs [block] while holding the GATT operation queue, so the app's own operations (issued on the
     * GATT inside [block], awaiting their own callbacks) never overlap with the gateway's. Keep blocks
     * short: gateway operations — e.g. writing desired configuration — wait meanwhile.
     *
     * @throws IllegalStateException before the first [start].
     */
    public suspend fun <T> runExclusive(block: suspend (BluetoothGatt) -> T): T {
        val current = checkNotNull(active) { "start() has not been called" }
        return current.connection.runExclusive(block)
    }

    /**
     * Stops relaying this device and tells it to stop streaming; the link itself is left to the host.
     * Data already buffered is still uploaded.
     */
    public fun detach() {
        active?.let { gateway.detach(it.gatt.device.address) }
    }
}

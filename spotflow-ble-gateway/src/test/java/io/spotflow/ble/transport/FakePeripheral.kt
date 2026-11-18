package io.spotflow.ble.transport

import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothGattService
import android.bluetooth.BluetoothStatusCodes
import android.os.Build
import io.mockk.every
import io.mockk.mockk
import io.spotflow.ble.protocol.GattProfile
import java.util.UUID

/**
 * A Spotflow peripheral behind a mocked [BluetoothGatt], for driving the real GATT engine in tests.
 *
 * Like the Android stack, it answers nothing by itself: every operation the code under test issues puts
 * the callback the stack would owe into [owed], and the test decides when — and whether — to [deliver]
 * it. The service tree is made of real framework objects.
 *
 * Android 13 changed how values travel: before it, a read or a notification leaves the value in the
 * characteristic and a write takes it from there; since then, values are passed as arguments. The fake
 * speaks whichever dialect the Android version under test does, so the same tests cover both code paths.
 */
internal class FakePeripheral(address: String = "AA:BB:CC:DD:EE:FF") {

    val capabilities = characteristic(GattProfile.CAPABILITIES)
    val deviceId = characteristic(GattProfile.DEVICE_ID)
    val sessionMetadata = characteristic(GattProfile.SESSION_METADATA)
    val tx = characteristic(GattProfile.TX_STREAM).apply {
        addDescriptor(BluetoothGattDescriptor(GattProfile.CCCD, BluetoothGattDescriptor.PERMISSION_WRITE))
    }
    val rx = characteristic(GattProfile.RX_STREAM)

    /** A characteristic of the host app's own, outside the Spotflow service. */
    val hostCharacteristic = characteristic(UUID.fromString("0000beef-0000-1000-8000-00805f9b34fb")).apply {
        addDescriptor(BluetoothGattDescriptor(GattProfile.CCCD, BluetoothGattDescriptor.PERMISSION_WRITE))
    }

    private val service = BluetoothGattService(GattProfile.SERVICE, BluetoothGattService.SERVICE_TYPE_PRIMARY).apply {
        listOf(capabilities, deviceId, sessionMetadata, tx, rx).forEach(::addCharacteristic)
    }

    /** What reading each characteristic returns. */
    val values = hashMapOf(
        GattProfile.CAPABILITIES to byteArrayOf(1, 1, 0),
        GattProfile.DEVICE_ID to "attached-device".toByteArray(),
        GattProfile.SESSION_METADATA to "meta".toByteArray(),
    )

    /** Every value written to the TX Stream's CCCD, in order. */
    val cccdWrites = ArrayList<ByteArray>()

    /** Every frame written to the RX Stream, in order. */
    val rxWrites = ArrayList<ByteArray>()

    /** The callbacks the stack owes for the operations issued so far, oldest first. */
    val owed = ArrayDeque<(BluetoothGattCallback) -> Unit>()

    val gatt: BluetoothGatt = mockk(relaxed = true)

    /** Whether values are passed as arguments (Android 13+) rather than through the characteristic. */
    private val modern = Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU

    init {
        val device = mockk<BluetoothDevice>()
        every { device.address } returns address
        every { gatt.device } returns device
        every { gatt.getService(GattProfile.SERVICE) } returns service
        every { gatt.discoverServices() } answers {
            owed += { it.onServicesDiscovered(gatt, BluetoothGatt.GATT_SUCCESS) }
            true
        }
        every { gatt.requestMtu(any()) } answers {
            owed += { it.onMtuChanged(gatt, 247, BluetoothGatt.GATT_SUCCESS) }
            true
        }
        every { gatt.readCharacteristic(any()) } answers {
            val characteristic = firstArg<BluetoothGattCharacteristic>()
            owed += { read(it, characteristic, values.getValue(characteristic.uuid)) }
            true
        }
        every { gatt.setCharacteristicNotification(any(), any()) } returns true
        if (modern) {
            every { gatt.writeDescriptor(any(), any()) } answers {
                val descriptor = firstArg<BluetoothGattDescriptor>()
                cccdWrites += secondArg<ByteArray>()
                owed += { it.onDescriptorWrite(gatt, descriptor, BluetoothGatt.GATT_SUCCESS) }
                BluetoothStatusCodes.SUCCESS
            }
            every { gatt.writeCharacteristic(any(), any(), any()) } answers {
                val characteristic = firstArg<BluetoothGattCharacteristic>()
                rxWrites += secondArg<ByteArray>()
                owed += { it.onCharacteristicWrite(gatt, characteristic, BluetoothGatt.GATT_SUCCESS) }
                BluetoothStatusCodes.SUCCESS
            }
        } else {
            @Suppress("DEPRECATION")
            every { gatt.writeDescriptor(any()) } answers {
                val descriptor = firstArg<BluetoothGattDescriptor>()
                cccdWrites += descriptor.value.copyOf()
                owed += { it.onDescriptorWrite(gatt, descriptor, BluetoothGatt.GATT_SUCCESS) }
                true
            }
            @Suppress("DEPRECATION")
            every { gatt.writeCharacteristic(any()) } answers {
                val characteristic = firstArg<BluetoothGattCharacteristic>()
                rxWrites += characteristic.value.copyOf()
                owed += { it.onCharacteristicWrite(gatt, characteristic, BluetoothGatt.GATT_SUCCESS) }
                true
            }
        }
        every { gatt.readRemoteRssi() } answers {
            owed += { it.onReadRemoteRssi(gatt, -60, BluetoothGatt.GATT_SUCCESS) }
            true
        }
    }

    /** Delivers the oldest owed callback to [callback]; false if nothing is owed. */
    fun deliver(callback: BluetoothGattCallback): Boolean {
        val next = owed.removeFirstOrNull() ?: return false
        next(callback)
        return true
    }

    /** Delivers the result of reading [characteristic] to [callback], the way this Android version does. */
    @Suppress("DEPRECATION")
    fun read(callback: BluetoothGattCallback, characteristic: BluetoothGattCharacteristic, value: ByteArray) {
        if (modern) {
            callback.onCharacteristicRead(gatt, characteristic, value, BluetoothGatt.GATT_SUCCESS)
        } else {
            characteristic.value = value
            callback.onCharacteristicRead(gatt, characteristic, BluetoothGatt.GATT_SUCCESS)
        }
    }

    /** Delivers a notification of [characteristic] to [callback], the way this Android version does. */
    @Suppress("DEPRECATION")
    fun notify(callback: BluetoothGattCallback, characteristic: BluetoothGattCharacteristic, value: ByteArray) {
        if (modern) {
            callback.onCharacteristicChanged(gatt, characteristic, value)
        } else {
            characteristic.value = value
            callback.onCharacteristicChanged(gatt, characteristic)
        }
    }

    /** Drops the oldest owed callback, as a peripheral that never answers would. */
    fun swallow() {
        owed.removeFirst()
    }

    private fun characteristic(uuid: UUID) = BluetoothGattCharacteristic(
        uuid,
        BluetoothGattCharacteristic.PROPERTY_READ or BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE or
            BluetoothGattCharacteristic.PROPERTY_NOTIFY,
        BluetoothGattCharacteristic.PERMISSION_READ or BluetoothGattCharacteristic.PERMISSION_WRITE,
    )
}

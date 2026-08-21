# Spotflow Android BLE Gateway

A reusable Android library (Kotlin, shipped as an AAR) that turns any Android app into a **Spotflow BLE
gateway**: it connects to devices running the [Spotflow Device SDK](https://github.com/spotflow-io/device-sdk)
with the BLE transport enabled, and relays their diagnostics to the Spotflow cloud over MQTT — including
**while the screen is off**, via a foreground service.

The goal is drop-in integration for hardware vendors who already ship an Android companion app (e.g. a
smart thermostat): add the library, hand it a connection or let it scan, and their BLE devices show up in
Spotflow.

## Modules

| Module | Purpose |
| --- | --- |
| `spotflow-ble-gateway` | The reusable library (`io.spotflow.ble`), published as an AAR. |
| `app` | A thin reference gateway app that uses the library. |

## How it works

```
Spotflow device (BLE peripheral)  ──GATT──▶  this library  ──MQTTS──▶  mqtt.spotflow.io:8883
   Observability service                       reassemble frames         username = device ID
   26530001-…-2C92E6887922                      relay CBOR payloads       password = ingest key
```

- **GATT** service `26530001-81E5-4861-82AE-2C92E6887922`, characteristics: Capabilities (`0002`),
  Device ID (`0003`), Session Metadata (`0004`), TX Stream NOTIFY (`0005`), RX Stream WRITE-no-response
  (`0006`).
- **Framing** — messages are fragmented across a possibly-tiny MTU (≥23 bytes). `FrameCodec` handles
  fragmentation (outgoing desired-config) and reassembly (incoming telemetry / reported-config).
- **Cloud** — one MQTT-over-TLS connection per device to `mqtt.spotflow.io:8883`, QoS 1, TLS validated by
  the Android system trust store (Let's Encrypt ISRG Root X1 — no bundled CA). Telemetry and session
  metadata publish to `ingest-cbor`; reported config to `config-cbor-d2c`; desired config is received from
  `config-cbor-c2d` and written back down the RX Stream.

### Package layout (`spotflow-ble-gateway/src/main/java/io/spotflow/ble`)

- `protocol/` — `GattProfile` (UUIDs), `MessageType`, `Message`, `FrameCodec` (pure JVM, unit-tested).
- `transport/` — `BleConnection` and the two modes below, `SpotflowGattSession` (serialized GATT command
  queue + callback bridge), `SpotflowScanner`.
- `cloud/` — `MqttUplink`, `MqttConfig`/`SpotflowTopics`, `CredentialsProvider` + `StaticIngestKey`.
- `SpotflowGateway`, `GatewaySession` — orchestration.
- `service/SpotflowGatewayService` — foreground service (`connectedDevice`).

## Two ways to feed devices

### Managed mode (library owns the connection)

```kotlin
val gateway = SpotflowGateway(context, StaticIngestKey("<ingest-key>"))
gateway.startScanning() // scans for the Spotflow service, connects, relays, reconnects with backoff
```

### Attach mode (host already owns the connection)

For apps that already hold a `BluetoothGatt` to the same device and don't want a second connection.

```kotlin
val connection = gateway.attach(existingGatt)
```

**Host contract for attach mode:**
1. Forward your `BluetoothGattCallback` events to `connection.gattCallback` (use it directly as your
   `connectGatt` callback, or fan out to it from your own callback).
2. Do **not** issue competing GATT operations while a Spotflow session is active — the Android BLE stack
   allows only one outstanding GATT operation at a time.
3. The host owns connect/disconnect and reconnection; `close()` only detaches.

## Background operation (screen off)

Run the gateway from the bundled foreground service:

```kotlin
SpotflowGatewayService.gatewayFactory = { ctx -> SpotflowGateway(ctx, StaticIngestKey(key)) }
SpotflowGatewayService.onReady = { it.startScanning() }
SpotflowGatewayService.start(context)
```

It runs as foreground service type `connectedDevice` with a persistent (customizable) notification. For a
dedicated always-on gateway, also guide users to disable battery optimization (Doze) for the app.

## Building

Requires **JDK 17** and the **Android SDK** (`compileSdk 35`). Point Gradle at the SDK via a
`local.properties` with `sdk.dir=/path/to/Android/sdk` (or the `ANDROID_HOME` env var).

```bash
gradle wrapper            # first time only, generates ./gradlew
./gradlew :spotflow-ble-gateway:test          # run FrameCodec unit tests (no device needed)
./gradlew :spotflow-ble-gateway:assembleRelease  # build the AAR
./gradlew :app:assembleDebug                  # build the demo app
```

## Verifying

1. **Unit (no hardware):** `FrameCodecTest` covers fragmentation/reassembly incl. 23-byte MTU,
   multi-fragment, and malformed input.
2. **Without hardware:** emulate the peripheral with **nRF Connect for Mobile** (GATT server advertising
   the Spotflow service/characteristics) or a second Android device; run the demo app and confirm frames
   reassemble and telemetry lands in the Spotflow cloud UI.
3. **End-to-end:** flash the Device SDK BLE sample on an ESP32-C3/C6, run the demo app, turn the screen
   off, and verify diagnostics keep arriving.

## Validation status

Verified end-to-end on 2026-08-21: a Pixel 9 (Android 15) gatewaying a Silicon Labs EFR32MG24 running
the Spotflow BLE bridge firmware — BLE connect, Device ID read, frame reassembly, MQTTS publish, data in
the Spotflow cloud, and continued relaying with the screen off. This confirmed the previously-open items
against real firmware: default literal MQTT topics work, the assumed frame flag bits are correct, and the
broker accepts MQTT 5.

Still unproven (didn't arise in that test), worth a follow-up:
- **Multi-fragment reassembly across a real MTU split** — telemetry was likely single-fragment; unit tests
  cover the logic but no on-device large payload was seen.
- **Desired-config downlink** (MQTT `config-cbor-c2d` → RX Stream write).
- **Attach mode** (`AttachedBleConnection`) on a device — only managed mode was exercised.

## Not in this iteration

Application-level ACK/retry (protocol defines it but doesn't implement it), coredump relay (MQTT-only in
the Device SDK), publishing the AAR to Maven Central, and any UI beyond the demo.

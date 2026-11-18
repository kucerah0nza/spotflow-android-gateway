# Changelog

## 0.2.1

Support for version 1.1 of the Spotflow BLE protocol, which comes with the current Spotflow cloud and
the Device SDK's BLE OTA support. The MQTT side (broker, credentials, topics) is unchanged. OTA updates
through the gateway are not relayed yet.

### Added

- **Protocol 1.1** — the Capabilities characteristic is read as major and minor version plus the feature
  byte (bit 0: OTA supported); 1.0 devices, which expose only the version, keep working. A device built
  with OTA is relayed for telemetry and configuration, and the log says that its OTA is not relayed.
- The new message types (`OTA_D2C`, `OTA_C2D`, `ARTIFACT_REQUEST`, `ARTIFACT_RESPONSE`) and the
  `ACK`/`NACK` control frames are recognized and dropped on arrival, instead of being reassembled (taking
  up to 64 KiB each and competing with telemetry for the in-flight slots) and dropped at the end.

### Fixed

- **Session metadata** is published again after an MQTT reconnect, ahead of everything else: the cloud
  applies it per connection, so telemetry sent after a reconnect lost its context (build ID, run ID).
- A message whose fragments disagree on `NEEDS_ACK` is dropped as malformed, as protocol 1.1 requires.
- A device that requires an encrypted link (the Device SDK's default when built with OTA) fails with an
  error saying it must be paired, not with a bare GATT status.

## 0.2.0

A hardening release. The library's public API changed in a few places; see
[Migrating from 0.1.x](#migrating-from-01x).

### Fixed

- **MQTT** — a connect that is cancelled no longer leaves a connected client behind that never
  subscribed (desired configuration then never arrived). A connection lost while messages are in flight
  is noticed at once instead of after a 20 s timeout, and the client no longer re-sends them behind the
  upload loop's back. A connection lost before the subscription is confirmed fails the connect instead of
  hanging for about an hour.
- **Discovery** — a scan that fails is retried with backoff (it used to stay dead until Bluetooth was
  toggled), and a running scan is restarted before Android throttles it.
- **Storage** — the buffer's database is opened in the background, not on the thread that creates the
  gateway. If it can't be opened the gateway relays with the RAM tier only instead of crashing.
- **Restart** — data the previous gateway writes to flash after the new one has started is picked up.
  Restarting with the flash tier switched off no longer wipes the backlog.
- **Sessions** — a BLE session lets go of its cloud link before the link is handed on, and two sessions
  can no longer both claim one device ID.
- **Attach mode** — callbacks only complete the operation they answer; detaching turns the device's
  stream off; a link that drops while the stream is being started is not reported as ready.
- **Poison messages** — a connection lost to the network, or a broker over its quota, never counts
  against a message; with several messages in flight, a bad one no longer takes its neighbours with it.
- **ProGuard / R8** — the library's consumer rules now let a shrunk build through (it failed on missing
  SLF4J classes).

### Added

- `SpotflowGateway.status`: whether discovery is running (and why not), connectivity, storage.
- `DeviceError`: a typed error per device (`AUTH_REJECTED`, `CLOUD_UNREACHABLE`, `BLUETOOTH`,
  `DUPLICATE_DEVICE`).
- `SpotflowGateway.retryStopped()`: retries devices stopped by a rejected key or the device filter,
  devices that were detached, and stored data whose upload gave up.
- `SpotflowLog`: route, filter or silence the library's logging.
- `SpotflowGatewayService.gatewayFlow`: follow the hosted gateway across restarts.
- Releases attach a Maven repository zip, so the library's dependencies resolve.

### Changed

- Up to 16 messages are uploaded at once; flash is read and cleared in batches.
- Uploads wait for a network instead of retrying without one.
- The scan runs low-power after its first two minutes; signal strength is read only while `devices` is
  being collected; the MQTT keep-alive default is 60 s (was 30 s).
- A rejected ingest key is retried after a pause that grows from 1 to 15 minutes (it used to stop for
  good); a device the filter rejects is asked about again after 10 minutes.
- `detach(address)` keeps a managed device out until `retryStopped()` (the scan used to reconnect it).
- Buffer sizes include an estimate of the per-message overhead.
- The library no longer declares Bluetooth LE as a *required* feature.
- Licensed under the Business Source License 1.1, like the Spotflow Device SDK.
- Built with AGP 8.13, Kotlin 2.3 and Gradle 8.14; the MQTT client is HiveMQ 1.4.2.

### Migrating from 0.1.x

- **Constructor** — `deviceFilter` is now required and comes third:

  ```kotlin
  // before
  SpotflowGateway(context, StaticIngestKey(key), mqttConfig)
  // after
  SpotflowGateway(context, StaticIngestKey(key), myFilter, mqttConfig)   // DeviceFilter.AcceptAll to relay everything
  ```

- **Attach mode** — `attach(gatt)` is replaced by a handle you wire up before starting:

  ```kotlin
  // before
  val connection = gateway.attach(gatt)            // then forward callbacks to connection.gattCallback
  // after
  val attachment = gateway.newAttachment()
  // pass attachment.gattCallback to connectGatt, or forward every callback to it, then:
  attachment.start(gatt)
  ```

  `connection.runExclusive { }` becomes `attachment.runExclusive { }`; `close()` becomes `detach()`.

- **Errors** — `GatewayDeviceState.error` is a `DeviceError?` (was `String?`): use `error.kind` to decide
  and `error.message` to display.
- **MQTT configuration** — `MqttConfig.qos` is the library's own `PublishQos` (was HiveMQ's `MqttQos`).
- **Internal types** — `MqttUplink`, `Uplink`, `SpotflowScanner`, `ManagedBleConnection`,
  `AttachedBleConnection`, `BleConnection`, `FrameCodec`, `Message` and `MessageType` are no longer
  public. `GattProfile` (the UUIDs) still is.
- **Dependencies** — if you take the bare `.aar`, see "Adding the library" in the README for what to add;
  the MQTT client is now `com.hivemq:hivemq-mqtt-client:1.4.2`.
- **Manifest** — if your app needs Bluetooth LE, declare the feature as required yourself.

package io.spotflow.ble.cloud

/**
 * The broker rejected the credentials (e.g. a bad ingest key). Not retryable with the same key, so the
 * gateway stops reconnecting the device until the host restarts it with new credentials.
 */
class MqttAuthException(message: String) : Exception(message)

/** The broker refused or failed the connection for a reason that may be transient. Retryable. */
class MqttConnectException(message: String, cause: Throwable? = null) : Exception(message, cause)

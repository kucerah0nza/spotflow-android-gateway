package io.spotflow.ble.cloud

import java.io.IOException

/**
 * The broker rejected the credentials (e.g. a bad ingest key). Retrying with the same key is pointless, so
 * the gateway only tries again after a long pause, asking the [CredentialsProvider] for the key anew.
 */
internal class MqttAuthException(message: String) : Exception(message)

/** The broker refused or failed the connection for a reason that may be transient. Retryable. */
internal class MqttConnectException(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 * The broker refused a publish because the workspace is over its quota: a matter of how much is being
 * sent, not of this message. Retried later; never a reason to drop the message.
 */
internal class UplinkThrottledException(message: String, cause: Throwable? = null) : IOException(message, cause)

/**
 * The connection ended while a publish was waiting for its acknowledgement, so it is unknown whether the
 * broker got the message.
 *
 * @param rejectedByBroker the broker closed the connection itself, and not for a reason of its own (such
 *   as shutting down or another client taking over the session) — which suggests it objects to what was
 *   just sent. A connection lost to the network never sets this.
 */
internal class UplinkDisconnectedException(
    message: String,
    val rejectedByBroker: Boolean,
    cause: Throwable? = null,
) : IOException(message, cause)

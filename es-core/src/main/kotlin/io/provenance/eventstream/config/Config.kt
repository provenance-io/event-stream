package io.provenance.eventstream.config

import com.sksamuel.hoplite.ConfigAlias

// Data classes in this file are intended to be instantiated by the hoplite configuration library

data class WebsocketStreamConfig(
    val uri: String,
    @ConfigAlias("throttle_duration_ms") val throttleDurationMs: Long = 0
)

data class RpcStreamConfig(val uri: String, val chainId: String)

data class StreamEventsFilterConfig(
    @ConfigAlias("tx_events") val txEvents: Set<String> = emptySet(),
    @ConfigAlias("block_events") val blockEvents: Set<String> = emptySet()
) {
    companion object {
        val empty: StreamEventsFilterConfig get() = StreamEventsFilterConfig()
    }
}

data class BatchConfig(
    val size: Int,
    @ConfigAlias("timeout_ms") val timeoutMillis: Long?,
)

data class EventStreamConfig(
    val websocket: WebsocketStreamConfig,
    val rpc: RpcStreamConfig,
    val batch: BatchConfig,
    val filter: StreamEventsFilterConfig = StreamEventsFilterConfig.empty,
    val skipEmpty: Boolean = true,
)

data class UploadConfig(
    val extractors: List<String> = emptyList()
) {
    companion object {
        val empty: UploadConfig get() = UploadConfig()
    }
}

data class Config(
    @ConfigAlias("event-stream") val eventStream: EventStreamConfig,
    val upload: UploadConfig = UploadConfig.empty
)

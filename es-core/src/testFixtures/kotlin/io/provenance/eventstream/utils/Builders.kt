package io.provenance.eventstream.test.utils

import com.squareup.moshi.Moshi
import io.provenance.eventstream.config.Options
import io.provenance.eventstream.coroutines.DispatcherProvider
import io.provenance.eventstream.stream.EventStreamService
import io.provenance.eventstream.stream.TendermintServiceClient
import io.provenance.eventstream.stream.EventStream
import io.provenance.eventstream.stream.clients.TendermintBlockFetcher
import io.provenance.eventstream.stream.models.BlockResultsResponse
import io.provenance.eventstream.stream.models.BlockchainResponse
import io.provenance.eventstream.test.mocks.MockEventStreamService
import io.provenance.eventstream.test.mocks.MockTendermintServiceClient
import io.provenance.eventstream.test.mocks.ServiceMocker
import kotlinx.coroutines.ExperimentalCoroutinesApi
import tendermint.abci.Types
import tendermint.types.BlockOuterClass.Block

@ExperimentalCoroutinesApi
object Builders {

    /**
     * Create a mock of the Tendermint service API exposed on Provenance.
     */
    fun tendermintService(): ServiceMocker.Builder = ServiceMocker.Builder()
        .doFor("abciInfo") {
            Defaults.templates.readAs(
                Types.ResponseInfo.newBuilder(),
                "abci_info/success.json",
                mapOf("last_block_height" to MAX_HISTORICAL_BLOCK_HEIGHT)
            )
        }
        .doFor("block") { Defaults.templates.readAs(Block.newBuilder(), "block/${it[0]}.json") }
        .doFor("blockResults") {
            Defaults.templates.readAs(
                BlockResultsResponse::class.java,
                "block_results/${it[0]}.json"
            )
        }
        .doFor("blockchain") {
            Defaults.templates.readAs(
                BlockchainResponse::class.java,
                "blockchain/${it[0]}-${it[1]}.json"
            )
        }

    /**
     * Create a mock of the Tendermint RPC event stream exposed on Provenance.
     */
    fun eventStreamService(includeLiveBlocks: Boolean = true): MockEventStreamService.Builder {
        val serviceBuilder = MockEventStreamService.Builder()
        if (includeLiveBlocks) {
            for (liveBlockResponse in Defaults.templates.readAll("live")) {
                serviceBuilder.response(liveBlockResponse)
            }
        }
        return serviceBuilder
    }

    /**
     * Create a mock of the Provenance block event stream.
     */
    data class EventStreamBuilder(val builders: Builders) {
        var dispatchers: DispatcherProvider? = null
        var eventStreamService: EventStreamService? = null
        var tendermintServiceClient: TendermintServiceClient? = null
        var moshi: Moshi? = null
        var options: Options.Builder = Options.Builder()
        var includeLiveBlocks: Boolean = true

        fun <T : EventStreamService> eventStreamService(value: T) = apply { eventStreamService = value }
        fun <T : TendermintServiceClient> tendermintService(value: T) = apply { tendermintServiceClient = value }
        fun moshi(value: Moshi) = apply { moshi = value }
        fun dispatchers(value: DispatcherProvider) = apply { dispatchers = value }
        fun options(value: Options.Builder) = apply { options = value }
        fun includeLiveBlocks(value: Boolean) = apply { includeLiveBlocks = value }

        // shortcuts for options:
        fun batchSize(value: Int) = apply { options.batchSize(value) }
        fun fromHeight(value: Long) = apply { options.fromHeight(value) }
        fun toHeight(value: Long) = apply { options.toHeight(value) }
        fun skipIfEmpty(value: Boolean) = apply { options.skipIfEmpty(value) }
        fun matchBlockEvent(predicate: (event: String) -> Boolean) = apply { options.matchBlockEvent(predicate) }
        fun matchTxEvent(predicate: (event: String) -> Boolean) = apply { options.matchTxEvent(predicate) }

        suspend fun build(): EventStream {
            val dispatchers = dispatchers ?: error("dispatchers must be provided")
            return EventStream(
                eventStreamService = eventStreamService
                    ?: builders
                        .eventStreamService(includeLiveBlocks = includeLiveBlocks)
                        .dispatchers(dispatchers)
                        .build(),
                fetcher = TendermintBlockFetcher(
                    tendermintServiceClient
                        ?: builders.tendermintService().build(MockTendermintServiceClient::class.java)
                ),
                moshi = moshi ?: Defaults.moshi,
                dispatchers = dispatchers,
                options = options.build()
            )
        }
    }

    fun eventStream(): EventStreamBuilder = EventStreamBuilder(this)
}

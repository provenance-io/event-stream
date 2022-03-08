package io.provenance.eventstream

import com.squareup.moshi.Moshi
import com.tinder.scarlet.Scarlet
import com.tinder.scarlet.lifecycle.LifecycleRegistry
import io.provenance.eventstream.config.Config
import io.provenance.eventstream.config.Environment
import io.provenance.eventstream.config.Options
import io.provenance.eventstream.coroutines.DefaultDispatcherProvider
import io.provenance.eventstream.coroutines.DispatcherProvider
import io.provenance.eventstream.stream.EventStream
import io.provenance.eventstream.stream.TendermintEventStreamService
import io.provenance.eventstream.stream.TendermintRPCStream
import io.provenance.eventstream.stream.clients.TendermintBlockFetcher
import kotlinx.coroutines.ExperimentalCoroutinesApi

@ExperimentalCoroutinesApi
class Factory(
    private val config: Config,
    private val moshi: Moshi,
    private val eventStreamBuilder: Scarlet.Builder,
    private val fetcher: TendermintBlockFetcher,
    private val dispatchers: DispatcherProvider = DefaultDispatcherProvider(),
) {
    companion object {
        /**
         * Create an event stream factory for the given environment using default values for the configuration,
         * moshi, the event stream builder, and Tenderming service client
         *
         * @see [defaultConfig]
         * @see [defaultMoshi]
         * @see [defaultEventStreamBuilder]
         * @see [defaultTendermintService]
         * @return The configured event stream factory instance.
         */
        fun using(environment: Environment): Factory {
            val config = defaultConfig(environment)
            return Factory(
                config = config,
                moshi = defaultMoshi(),
                eventStreamBuilder = defaultEventStreamBuilder(config.eventStream.websocket.uri),
                fetcher = TendermintBlockFetcher(defaultTendermintService(config.eventStream.rpc.uri, config.eventStream.rpc.chainId))
            )
        }
    }

    private fun noop(_options: Options.Builder) {}

    /**
     * Creates a new event stream. Prior to the event stream being created, the closure will be passed an
     * [EventStream.Options.Builder] which can be used to modify the options passed to create the actual event stream,
     * e.g.
     *
     * ```kotlin
     * val eventStream: EventStream = Factory(Environment.LOCAL).createStream { optionsBuilder ->
     *   optionsBuilder.batchSize(8)
     *   optionsBuilder.skipIfEmpty(false)
     * }
     * ```
     *
     * By default, the following options are automatically set on the passed [EventStream.Options.Builder] instance:
     *
     * - `batchSize` = `config.eventStream.batch.size`
     * - `skipIfEmpty` = `true`
     *
     * @param setOptions The closure used to configure the [EventStream.Options.Builder] passed to it.
     * @return The created event stream instance.
     */
    fun createStream(setOptions: (options: Options.Builder) -> Unit = ::noop): EventStream {
        val optionsBuilder = Options.Builder()
            .batchSize(config.eventStream.batch.size)
            .skipIfEmpty(config.eventStream.skipEmpty)
        setOptions(optionsBuilder)
        return createStream(optionsBuilder.build())
    }

    /**
     * Creates a new event stream. The event stream is configured using a supplied [EventStream.Options] instance.
     *
     * @param options The event stream options [EventStream.Options] to use when creating the event stream.
     * @return The created event stream instance.
     */
    fun createStream(options: Options = Options.DEFAULT): EventStream {
        val lifecycle = LifecycleRegistry(config.eventStream.websocket.throttleDurationMs)
        val scarlet: Scarlet = eventStreamBuilder.lifecycle(lifecycle).build()
        val tendermintRpc: TendermintRPCStream = scarlet.create()
        val eventStreamService = TendermintEventStreamService(tendermintRpc, lifecycle)

        return EventStream(
            eventStreamService,
            fetcher,
            moshi,
            options = options,
            dispatchers = dispatchers
        )
    }
}

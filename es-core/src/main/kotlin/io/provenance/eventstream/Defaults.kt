package io.provenance.eventstream

import com.sksamuel.hoplite.ConfigLoader
import com.sksamuel.hoplite.EnvironmentVariablesPropertySource
import com.sksamuel.hoplite.PropertySource
import com.sksamuel.hoplite.preprocessor.PropsPreprocessor
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import com.tinder.scarlet.Scarlet
import com.tinder.scarlet.lifecycle.LifecycleRegistry
import com.tinder.scarlet.messageadapter.moshi.MoshiMessageAdapter
import com.tinder.scarlet.websocket.okhttp.newWebSocketFactory
import com.tinder.streamadapter.coroutines.CoroutinesStreamAdapterFactory
import io.provenance.blockchain.stream.api.BlockSource
import io.provenance.eventstream.adapter.json.JSONObjectAdapter
import io.provenance.eventstream.adapter.json.decoder.DecoderEngine
import io.provenance.eventstream.adapter.json.decoder.MoshiDecoderEngine
import io.provenance.eventstream.config.Config
import io.provenance.eventstream.config.Environment
import io.provenance.eventstream.stream.BlockStreamOptions
import io.provenance.eventstream.stream.DefaultBlockStreamFactory
import io.provenance.eventstream.stream.EventStreamService
import io.provenance.eventstream.stream.TendermintEventStreamService
import io.provenance.eventstream.stream.TendermintRPCStream
import io.provenance.eventstream.stream.TendermintServiceClient
import io.provenance.eventstream.stream.clients.TendermintBlockFetcher
import io.provenance.eventstream.stream.clients.TendermintServiceOpenApiClient
import io.provenance.eventstream.stream.models.StreamBlockImpl
import kotlinx.coroutines.ExperimentalCoroutinesApi
import okhttp3.OkHttpClient
import java.net.URI
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

/**
 * Create a default okHttpClient to use for the event stream.
 */
@OptIn(ExperimentalTime::class)
fun defaultOkHttpClient(pingInterval: Duration = Duration.seconds(10), readInterval: Duration = Duration.seconds(60)) =
    OkHttpClient.Builder()
        .pingInterval(pingInterval.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        .readTimeout(readInterval.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        .build()
/**
 * Create a default configuration to use for the event stream.
 *
 * @param environment The environment to generate a configutation with.
 * @return The configuration
 */
fun defaultConfig(environment: Environment): Config = ConfigLoader.Builder()
    .addSource(EnvironmentVariablesPropertySource(useUnderscoresAsSeparator = true, allowUppercaseNames = true))
    .apply {
        // If in the local environment, override the ${...} envvar values in `application.properties` with
        // the values provided in the local-specific `local.env.properties` property file:
        if (environment.isLocal()) {
            addPreprocessor(PropsPreprocessor("/local.env.properties"))
        }
    }
    .addSource(PropertySource.resource("/application.yml"))
    .build()
    .loadConfigOrThrow()

/**
 * Create a default websocket builder for the event stream.
 *
 * @param websocketUri The URI the websocket will listen on.
 */
@OptIn(ExperimentalTime::class)
fun defaultEventStreamBuilder(websocketUri: String, okHttpClient: OkHttpClient = defaultOkHttpClient()): Scarlet.Builder {
    val node = URI(websocketUri)
    return Scarlet.Builder()
        .webSocketFactory(okHttpClient.newWebSocketFactory("${node.scheme}://${node.host}:${node.port}/websocket"))
        .addMessageAdapterFactory(MoshiMessageAdapter.Factory())
        .addStreamAdapterFactory(CoroutinesStreamAdapterFactory())
}

/**
 * Create the default [Moshi] JSON serializer/deserializer.
 *
 * @return The [Moshi] instance to use for the event stream.
 */
fun defaultMoshi(): Moshi = Moshi.Builder()
    .add(KotlinJsonAdapterFactory())
    .add(JSONObjectAdapter())
    .build()

/**
 * Create the default [Moshi] [DecoderEngine]
 *
 * @param moshi The moshi instance to use in the decoder engine.
 * @return The [DecoderEngine] instance to use for the event stream.
 */
fun defaultDecoderEngine(moshi: Moshi = defaultMoshi()): DecoderEngine =
    MoshiDecoderEngine(moshi)

/**
 * Create the default [TendermintServiceClient] to use with the event stream.
 *
 * @param rpcUri The URI of the Tendermint RPC service to connect to.
 * @return The [TendermintServiceClient] instance to use for the event stream.
 */
fun defaultTendermintService(rpcUri: String): TendermintServiceClient =
    TendermintServiceOpenApiClient(rpcUri)

/**
 * Create the default [TendermintServiceClient] to use with the event stream.
 *
 * @param rpcUri The URI of the Tendermint RPC service to connect to.
 * @return The [TendermintServiceClient] instance to use for the event stream.
 */
fun defaultTendermintFetcher(rpcUri: String): TendermintBlockFetcher =
    TendermintBlockFetcher(TendermintServiceOpenApiClient(rpcUri))

/**
 *
 */
fun defaultEventStreamService(uri: String, okHttpClient: OkHttpClient, throttle: Duration): EventStreamService {
    val eventStreamBuilder = defaultEventStreamBuilder(uri, okHttpClient)
    val lifecycle = LifecycleRegistry(throttle.inWholeMilliseconds)
    val scarlet: Scarlet = eventStreamBuilder.lifecycle(lifecycle).build()
    val tendermintRpc: TendermintRPCStream = scarlet.create(TendermintRPCStream::class.java)
    return TendermintEventStreamService(tendermintRpc, lifecycle)
}

/**
 *
 */
@OptIn(ExperimentalCoroutinesApi::class, ExperimentalTime::class)
fun defaultEventStream(config: Config, options: BlockStreamOptions, okHttpClient: OkHttpClient = defaultOkHttpClient(), moshi: Moshi = defaultMoshi(), fetcher: TendermintBlockFetcher = defaultTendermintFetcher(config.eventStream.rpc.uri)): BlockSource<StreamBlockImpl> {
    val factory = DefaultBlockStreamFactory(
        config = config,
        decoderEngine = defaultDecoderEngine(moshi),
        eventStreamBuilder = defaultEventStreamBuilder(config.eventStream.websocket.uri, okHttpClient),
        blockFetcher = fetcher
    )

    return factory.createSource(options)
}

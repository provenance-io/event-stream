package io.provenance.eventstream

import com.sksamuel.hoplite.ConfigLoader
import com.sksamuel.hoplite.EnvironmentVariablesPropertySource
import com.sksamuel.hoplite.PropertySource
import com.sksamuel.hoplite.preprocessor.PropsPreprocessor
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import com.tinder.scarlet.Scarlet
import com.tinder.scarlet.messageadapter.moshi.MoshiMessageAdapter
import com.tinder.scarlet.websocket.okhttp.newWebSocketFactory
import com.tinder.streamadapter.coroutines.CoroutinesStreamAdapterFactory
import io.provenance.eventstream.adapter.json.JSONObjectAdapter
import io.provenance.eventstream.config.Config
import io.provenance.eventstream.config.Environment
import io.provenance.eventstream.config.Options
import io.provenance.eventstream.stream.EventStream
import io.provenance.eventstream.stream.TendermintServiceClient
import io.provenance.eventstream.stream.clients.TendermintBlockFetcher
import io.provenance.eventstream.stream.clients.TendermintServiceOpenApiClient
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
 * Create the default [TendermintServiceClient] to use with the event stream.
 *
 * @param rpcUri The URI of the Tendermint RPC service to connect to.
 * @return The [TendermintServiceClient] instance to use for the event stream.
 */
fun defaultTendermintService(rpcUri: String, chainId: String): TendermintServiceClient =
    TendermintServiceOpenApiClient(URI(rpcUri), chainId)

/**
 * Create the default [TendermintServiceClient] to use with the event stream.
 *
 * @param rpcUri The URI of the Tendermint RPC service to connect to.
 * @return The [TendermintServiceClient] instance to use for the event stream.
 */
fun defaultTendermintFetcher(rpcUri: String, chainId: String): TendermintBlockFetcher =
    TendermintBlockFetcher(TendermintServiceOpenApiClient(URI(rpcUri), chainId))

/**
 *
 */
@OptIn(ExperimentalCoroutinesApi::class, ExperimentalTime::class)
fun defaultEventStream(config: Config, options: Options, okHttpClient: OkHttpClient = defaultOkHttpClient(), moshi: Moshi = defaultMoshi(), fetcher: TendermintBlockFetcher = defaultTendermintFetcher(config.eventStream.rpc.uri, config.eventStream.rpc.chainId)): EventStream {
    val factory = Factory(
        config = config,
        moshi = moshi,
        eventStreamBuilder = defaultEventStreamBuilder(config.eventStream.websocket.uri, okHttpClient),
        fetcher = fetcher
    )

    return factory.createStream(options)
}

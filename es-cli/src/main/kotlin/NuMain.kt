package io.provenance.eventstream

import io.provenance.blockchain.stream.api.BlockSink
import io.provenance.eventstream.config.Config
import io.provenance.eventstream.config.EventStreamConfig
import io.provenance.eventstream.config.WebsocketStreamConfig
import io.provenance.eventstream.config.RpcStreamConfig
import io.provenance.eventstream.config.StreamEventsFilterConfig
import io.provenance.eventstream.config.BatchConfig
import io.provenance.eventstream.config.Options
import io.provenance.eventstream.observers.kafkaFileOutput
import io.provenance.eventstream.stream.acking
import io.provenance.eventstream.stream.infrastructure.Serializer.moshi
import io.provenance.eventstream.stream.models.StreamBlock
import io.provenance.eventstream.stream.observers.fileOutput
import io.provenance.eventstream.stream.kafkaBlockSource
import io.provenance.eventstream.stream.kafkaBlockSink
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.default
import kotlinx.coroutines.launch
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.flow.DEFAULT_CONCURRENCY
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.ExperimentalTime

val commonProps = { id: String? ->
    mapOf(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        CommonClientConfigs.CLIENT_ID_CONFIG to (id ?: UUID.randomUUID().toString()),
    )
}

val producerProps = mapOf(
    ProducerConfig.ACKS_CONFIG to "all",
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
)

val consumerProps = mapOf(
    ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 15,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 10000,
    ConsumerConfig.GROUP_ID_CONFIG to "test-group",
)

fun logger(name: String): org.slf4j.Logger = LoggerFactory.getLogger(name)
var org.slf4j.Logger.level: Level
    get() = (this as Logger).level
    set(value) {
        (this as Logger).level = value
    }

fun producerProps(id: String) = commonProps(id) + producerProps
fun consumerProps(id: String) = commonProps(id) + consumerProps

@OptIn(FlowPreview::class, ExperimentalTime::class, kotlinx.coroutines.InternalCoroutinesApi::class)
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    /**
     * All configuration options can be overridden via environment variables:
     *
     * - To override nested configuration options separated with a dot ("."), use double underscores ("__")
     *   in the environment variable:
     *     event.stream.rpc_uri=http://localhost:26657 is overridden by "event__stream_rpc_uri=foo"
     *
     * @see https://github.com/sksamuel/hoplite#environmentvariablespropertysource
     */
    val parser = ArgParser("provenance-event-stream")
    val fromHeight by parser.option(
        ArgType.String,
        fullName = "from",
        description = "Fetch blocks starting from height, inclusive."
    )
    val toHeight by parser.option(ArgType.String, fullName = "to", description = "Fetch blocks up to height, inclusive")
    val verbose by parser.option(
        ArgType.Boolean,
        fullName = "verbose",
        shortName = "v",
        description = "Enables verbose output"
    ).default(false)
    val node by parser.option(
        ArgType.String,
        fullName = "node",
        shortName = "n",
        description = "Node to connect to for block stream"
    ).default("localhost:26657")
    val ordered by parser.option(
        ArgType.Boolean,
        fullName = "ordered",
        shortName = "o",
        description = "Order incoming blocks"
    ).default(false)
    val batchSize by parser.option(ArgType.Int, fullName = "batch", shortName = "s", description = "Batch fetch size")
        .default(16)
    val throttle by parser.option(
        ArgType.Int,
        fullName = "throttle",
        shortName = "w",
        description = "Websocket throttle duration (milliseconds)"
    ).default(0)
    val timeout by parser.option(
        ArgType.Int,
        fullName = "timeout",
        shortName = "x",
        description = "History fetch timeout (milliseconds)"
    ).default(30000)
    val concurrency by parser.option(
        ArgType.Int,
        fullName = "concurrency",
        shortName = "c",
        description = "Concurrency limit for parallel fetches"
    ).default(DEFAULT_CONCURRENCY)
    val txFilter by parser.option(
        ArgType.String,
        fullName = "filter-tx",
        shortName = "t",
        description = "Filter by tx events (comma separated)"
    ).default("")
    val blockFilter by parser.option(
        ArgType.String,
        fullName = "filter-block",
        shortName = "b",
        description = "Filter by block events (comma separated)"
    ).default("")
    val chainId by parser.option(
        ArgType.String,
        fullName = "chainId",
        shortName = "i",
        description = "chain id"
    ).default("chain-local")
    val keepEmpty by parser.option(ArgType.Boolean, fullName = "keep-empty", description = "Keep empty blocks")
        .default(true)
    parser.parse(args)

    val config = Config(
        eventStream = EventStreamConfig(
            skipEmpty = !keepEmpty,
            websocket = WebsocketStreamConfig("ws://$node"),
            rpc = RpcStreamConfig("grpc://$node", chainId),
            filter = StreamEventsFilterConfig(
                txEvents = txFilter.split(",").filter { it.isNotBlank() }.toSet(),
                blockEvents = blockFilter.split(",").filter { it.isNotBlank() }.toSet()
            ),
            batch = BatchConfig(batchSize, timeoutMillis = timeout.toLong())
        ),
    )

    val log = KotlinLogging.logger {}
    if (config.eventStream.filter.txEvents.isNotEmpty()) {
        log.info("Listening for tx events:")
        for (event in config.eventStream.filter.txEvents) {
            log.info(" - $event")
        }
    }

    if (config.eventStream.filter.blockEvents.isNotEmpty()) {
        log.info("Listening for block events:")
        for (event in config.eventStream.filter.blockEvents) {
            log.info(" - $event")
        }
    }

    val options = Options(
        batchSize = batchSize,
        fromHeight = fromHeight?.toLong(),
        toHeight = toHeight?.toLong(),
        skipIfEmpty = config.eventStream.skipEmpty,
        txEventPredicate = { config.eventStream.filter.txEvents.isEmpty() || it in config.eventStream.filter.txEvents },
        blockEventPredicate = { config.eventStream.filter.blockEvents.isEmpty() || it in config.eventStream.filter.blockEvents },
        concurrency = concurrency,
    )

    val okClient = defaultOkHttpClient()
    val stream = defaultEventStream(config, options, okClient)
    System.setProperty("kotlinx.coroutines.debug", "on")

    runBlocking {
        log.info("config: $config")

        val chainStreamFlow = stream.streamBlocks()
            .flowOn(Dispatchers.IO)
            .buffer()
            .catch { log.error("", it) }
            .onCompletion { log.info("stream fetch complete", it) }
            .onEach(fileOutput("../pio-testnet-1/json-data", moshi))
            .onEach(kafkaBlockSink(producerProps("producer"), "test"))

        val kafkaStreamFlow = kafkaBlockSource(consumerProps("test"), "test").streamBlocks()
            .flowOn(Dispatchers.IO)
            .buffer()
            .catch { log.error("", it) }
            .onEach {
                kafkaFileOutput("../pio-testnet-1-kafka/json-data").invoke(it)
            }
            .acking {
                log.info("successfully acked:$it")
            }

        launch {
            chainStreamFlow.collect {
                log.info("recv from Chain: $it :: ${it.block} -> ${it.height}")
            }
        }

        launch {
            kafkaStreamFlow.collect {
                log.info("recv from Kafka: $it :: ${it.block} -> ${it.height}")
            }
        }
    }

    // OkHttp leaves a non-daemon executor running in the background. Have to explicitly shut down
    // the pool in order for the app to gracefully exit.
    okClient.dispatcher.executorService.shutdownNow()
    okClient.dispatcher.executorService.awaitTermination(3, TimeUnit.SECONDS)
}

private suspend fun Flow<StreamBlock>.collectTo(sink: BlockSink) =
    collect { sink(it) }

private fun Flow<StreamBlock>.onEach(block: BlockSink) =
    onEach { block(it) }

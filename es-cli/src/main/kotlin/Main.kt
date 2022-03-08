package io.provenance.eventstream

import io.provenance.eventstream.config.Config
import io.provenance.eventstream.config.Environment
import io.provenance.eventstream.config.Options
import io.provenance.eventstream.extensions.dateTime
import io.provenance.eventstream.extensions.repeatDecodeBase64
import io.provenance.eventstream.stream.consumers.EventStreamViewer
import io.provenance.eventstream.stream.models.StreamBlock
import io.provenance.eventstream.stream.models.extensions.dateTime
import io.provenance.eventstream.utils.colors.green
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.default
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

@OptIn(FlowPreview::class, kotlin.time.ExperimentalTime::class)
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    /**
     * All configuration options can be overridden via environment variables:
     *
     * - To override nested configuration options separated with a dot ("."), use double underscores ("__")
     *   in the environment variable:
     *     event.stream.rpc_uri=http://localhost:26657 is overridden by "event__stream_rpc_uri=foo"
     *
     * See https://github.com/sksamuel/hoplite#environmentvariablespropertysource
     */
    val parser = ArgParser("provenance-event-stream")
    val envFlag by parser.option(
        ArgType.Choice<Environment>(),
        shortName = "e",
        fullName = "env",
        description = "Specify the application environment. If not present, fall back to the `\$ENVIRONMENT` envvar",
    )
    val fromHeight by parser.option(
        ArgType.Int,
        fullName = "from",
        description = "Fetch blocks starting from height, inclusive."
    )
    val toHeight by parser.option(
        ArgType.Int, fullName = "to", description = "Fetch blocks up to height, inclusive"
    )
    val verbose by parser.option(
        ArgType.Boolean, shortName = "v", fullName = "verbose", description = "Enables verbose output"
    ).default(false)
    val skipIfEmpty by parser.option(
        ArgType.Choice(listOf(false, true), { it.toBooleanStrict() }),
        fullName = "skip-if-empty",
        description = "Skip blocks that have no transactions"
    ).default(true)

    parser.parse(args)

    val environment: Environment =
        envFlag ?: runCatching { Environment.valueOf(System.getenv("ENVIRONMENT")) }
            .getOrElse {
                error("Not a valid environment: ${System.getenv("ENVIRONMENT")}")
            }

    val config: Config = defaultConfig(environment)

    val log = KotlinLogging.logger { }

    runBlocking(Dispatchers.IO) {

        log.info(
            """
            |Running with options: {
            |    from-height = $fromHeight 
            |    to-height = $toHeight
            |    skip-if-empty = $skipIfEmpty
            |}
            """.trimMargin("|")
        )

        val options = Options
            .Builder()
            .batchSize(config.eventStream.batch.size)
            .fromHeight(fromHeight?.toLong())
            .toHeight(toHeight?.toLong())
            .skipIfEmpty(skipIfEmpty)
            .apply {
                if (config.eventStream.filter.txEvents.isNotEmpty()) {
                    matchTxEvent { it in config.eventStream.filter.txEvents }
                }
            }
            .apply {
                if (config.eventStream.filter.blockEvents.isNotEmpty()) {
                    matchBlockEvent { it in config.eventStream.filter.blockEvents }
                }
            }
            .also {
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
            }
            .build()

        val factory = Factory.using(environment)

        log.info("*** Observing blocks and events. No action will be taken. ***")
        EventStreamViewer(factory, options)
            .consume { b: StreamBlock ->
                val text = "Block: ${b.block.header?.height ?: "--"}: ${b.block.header?.dateTime()?.toLocalDate()
                } ${b.block.header?.lastBlockId?.hash}; ${b.txEvents.size} tx event(s)"
                println(
                    if (b.historical) {
                        text
                    } else {
                        green(text)
                    }
                )
                if (verbose) {
                    for (event in b.blockEvents) {
                        println("  Block-Event: ${event.type}")
                        for (attr in event.attributesList) {
                            println("    ${attr.key?.toStringUtf8()?.repeatDecodeBase64()}: ${attr.value?.toStringUtf8()?.repeatDecodeBase64()}")
                        }
                    }
                    for (event in b.txEvents) {
                        println("  Tx-Event: ${event.type}")
                        for (attr in event.attributesList) {
                            println("    ${attr.key?.repeatDecodeBase64()}: ${attr.value?.repeatDecodeBase64()}")
                        }
                    }
                }
                println()
            }
    }
}

package io.provenance.eventstream.stream.observers

import cosmos.base.abci.v1beta1.Abci
import io.provenance.blockchain.stream.api.BlockSink
import io.provenance.eventstream.extensions.dateTime
import io.provenance.eventstream.extensions.decodeBase64
import io.provenance.eventstream.extensions.isAsciiPrintable
import io.provenance.eventstream.stream.models.Event
import io.provenance.eventstream.stream.models.TxEvent
import io.provenance.eventstream.stream.models.BlockEvent
import io.provenance.eventstream.stream.models.StreamBlockImpl
import io.provenance.eventstream.stream.models.StreamBlock
import mu.KotlinLogging
import tendermint.abci.Types

fun consoleOutput(verbose: Boolean, nth: Int = 100): ConsoleOutput = ConsoleOutput(verbose, nth)

class ConsoleOutput(private val verbose: Boolean, private val nth: Int) : BlockSink {
    private val log = KotlinLogging.logger {}

    private val logAttribute: (Abci.Attribute) -> Unit = {
        log.info { "    ${it.key?.repeatDecodeBase64()}: ${it.value?.repeatDecodeBase64()}" }
    }

    private val logAttributeBlock: (Types.EventAttribute) -> Unit = {
        log.info { "    ${it.key?.toStringUtf8()?.repeatDecodeBase64()}: ${it.value?.toStringUtf8()?.repeatDecodeBase64()}" }
    }

    private val logBlockTxEvent: (Abci.StringEvent) -> Unit = {
        log.info { "  Tx-Event: ${it.type}" }
        it.attributesList.forEach(logAttribute)
    }

    private val logBlockEvent: (Types.Event) -> Unit = {
        log.info { "  Block-Event: ${it.type}" }
        it.attributesList.forEach(logAttributeBlock)
    }

    private val logBlockInfo: StreamBlockImpl.() -> Unit = {
        val height = block.header?.height ?: "--"
        val date = block.header?.dateTime()?.toLocalDate()
        val hash = block.header?.lastBlockId?.hash
        val size = txEvents.size
        log.info { "Block: $height: $date $hash; $size tx event(s)" }
    }

    override suspend fun invoke(block: StreamBlock) {
        if (block.height!! % nth != 0L) {
            return
        }

        (block as StreamBlockImpl).logBlockInfo()
        if (verbose) {
            block.txEvents.forEach(logBlockTxEvent)
            block.blockEvents.forEach(logBlockEvent)
        }
    }
}

/**
 * Decodes a string repeatedly base64 encoded, terminating when:
 *
 * - the decoded string stops changing or
 * - the maximum number of iterations is reached
 * - or the decoded string is no longer ASCII printable
 *
 * In the event of failure, the last successfully decoded string is returned.
 */
private fun String.repeatDecodeBase64(): String {
    var s: String = this.toString() // copy
    var t: String = s.decodeBase64().stripQuotes()
    repeat(10) {
        if (s == t || !t.isAsciiPrintable()) {
            return s
        }
        s = t
        t = t.decodeBase64().stripQuotes()
    }
    return s
}

/**
 * Remove surrounding quotation marks from a string.
 */
private fun String.stripQuotes(): String = this.removeSurrounding("\"")

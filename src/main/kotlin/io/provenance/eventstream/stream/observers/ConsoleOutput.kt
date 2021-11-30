package io.provenance.eventstream.stream.observers

import io.provenance.eventstream.extensions.decodeBase64
import io.provenance.eventstream.extensions.isAsciiPrintable
import io.provenance.eventstream.info
import io.provenance.eventstream.logger
import io.provenance.eventstream.stream.consumers.BlockSink
import io.provenance.eventstream.stream.models.BlockEvent
import io.provenance.eventstream.stream.models.Event
import io.provenance.eventstream.stream.models.StreamBlock
import io.provenance.eventstream.stream.models.TxEvent
import io.provenance.eventstream.stream.models.extensions.dateTime

fun consoleOutput(verbose: Boolean) = ConsoleOutput(verbose)

class ConsoleOutput(private val verbose: Boolean) : BlockSink {
    private val log = logger()

    private val logAttribute: (Event) -> Unit = {
        log.info { "    ${it.key?.repeatDecodeBase64()}: ${it.value?.repeatDecodeBase64()}" }
    }

    private val logBlockTxEvent: (TxEvent) -> Unit = {
        log.info { "  Tx-Event: ${it.eventType}" }
        it.attributes.forEach(logAttribute)
    }

    private val logBlockEvent: (BlockEvent) -> Unit = {
        log.info { "  Block-Event: ${it.eventType}" }
        it.attributes.forEach(logAttribute)
    }

    private val logBlockInfo: StreamBlock.() -> Unit = {
        val height = block.header?.height ?: "--"
        val date = block.header?.dateTime()?.toLocalDate()
        val hash = block.header?.lastBlockId?.hash
        val size = txEvents.size
        log.info { "Block: $height: $date $hash; $size tx event(s)" }
    }

    override fun invoke(block: StreamBlock) {
        block.logBlockInfo()
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


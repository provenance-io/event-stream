package io.provenance.eventstream.stream.flows

import io.provenance.eventstream.decoder.DecoderAdapter
import io.provenance.eventstream.net.NetAdapter
import io.provenance.eventstream.stream.clients.BlockData
import io.provenance.eventstream.stream.models.BlockHeader
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicLong

/**
 * Create a [Flow] of [BlockHeader] from height to height.
 *
 * This flow will intelligently determine how to merge the live and history flows to
 * create a seamless stream of [BlockHeader] objects.
 *
 * @param netAdapter The [NetAdapter] to use for network interfacing.
 * @param decoderAdapter The [DecoderAdapter] to use to marshal json.
 * @param from The `from` height, if omitted, height 1 is used.
 * @param to The `to` height, if omitted, no end is assumed.
 * @return The [Flow] of [BlockHeader].
 */
fun blockHeaderFlow(netAdapter: NetAdapter, decoderAdapter: DecoderAdapter, from: Long? = null, to: Long? = null): Flow<BlockHeader> {
    val getCurrentHeight: suspend () -> Long? = { netAdapter.rpcAdapter.getCurrentHeight() }
    val getHeight: (BlockHeader) -> Long = { it.height }
    val history: (Long, Long) -> Flow<BlockHeader> = { f, t -> historicalBlockHeaderFlow(netAdapter, f, t) }
    val live: () -> Flow<BlockHeader> = { liveBlockHeaderFlow(netAdapter, decoderAdapter) }
    return combinedFlow(getCurrentHeight, from, to, getHeight, history, live)
}

/**
 * Create a [Flow] of [BlockData] from height to height.
 *
 * This flow will intelligently determine how to merge the live and history flows to
 * create a seamless stream of [BlockData] objects.
 *
 * @param netAdapter The [NetAdapter] to use for network interfacing.
 * @param decoderAdapter The [DecoderAdapter] to use to marshal json.
 * @param from The `from` height, if omitted, height 1 is used.
 * @param to The `to` height, if omitted, no end is assumed.
 * @return The [Flow] of [BlockData].
 */
fun blockDataFlow(netAdapter: NetAdapter, decoderAdapter: DecoderAdapter, from: Long? = null, to: Long? = null): Flow<BlockData> {
    val getCurrentHeight: suspend () -> Long? = { netAdapter.rpcAdapter.getCurrentHeight() }
    val getHeight: (BlockData) -> Long = { it.height }
    val history: (Long, Long) -> Flow<BlockData> = { f, t -> historicalBlockDataFlow(netAdapter, f, t) }
    val live: () -> Flow<BlockData> = { liveBlockDataFlow(netAdapter, decoderAdapter) }
    return combinedFlow(getCurrentHeight, from, to, getHeight, history, live)
}

/**
 *
 */
@OptIn(ExperimentalCoroutinesApi::class)
private fun <T> combinedFlow(
    getCurrentHeight: suspend () -> Long?,
    from: Long? = null,
    to: Long? = null,
    getHeight: (T) -> Long,
    historicalFlow: (from: Long, to: Long) -> Flow<T>,
    liveFlow: () -> Flow<T>
): Flow<T> = channelFlow {
    val parent = this
    val log = KotlinLogging.logger {}
    val channel = Channel<T>(capacity = 10_000) // buffer for: 10_000 * 6s block time / 60s/m / 60m/h == 16 2/3 hrs buffer time.
    val liveJob = launch {
        liveFlow.invoke()
            .buffer()
            .collect { channel.send(it) }
    }

    val current = getCurrentHeight()!!
    log.debug { "hist//live split point:$current" }

    // Determine if we need live data.
    // ie: if to is null, or more than current,
    val needLive = to == null || to > current

    // Determine if we need historical data.
    // ie: if from is null, or less than current, then we do.
    val needHist = from == null || from < current
    log.debug { "from:$from to:$to current:$current needHist:$needHist needLive:$needLive" }

    // Cancel live job and channel if unneeded.
    if (!needLive) {
        liveJob.cancel()
        channel.close()
    }

    // Process historical stream if needed.
    val lastSeen = AtomicLong(0)
    if (needHist) {
        historicalFlow(from ?: 1, current)
            .collect {
                send(it)
                lastSeen.set(getHeight(it))
            }
    }

    // Live flow. Skip any dupe blocks.
    if (needLive) {
        // Continue receiving everything else live.
        // Drop anything between current head and the last fetched history record.
        while (!channel.isClosedForReceive) {
            gapFill(channel, getHeight, current, historicalFlow).collect { block ->
                select {
                    onSend(block) {
                        lastSeen.set(getHeight(block))
                    }
                }
            }

            channel.receiveAsFlow().dropWhile { getHeight(it) <= lastSeen.get() }.collect { block ->
                val collector: suspend (T) -> Unit = { b ->
                    select {
                        onSend(b) {
                            lastSeen.set(getHeight(b))
                            if (to != null && getHeight(b) >= to) {
                                parent.close()
                            }
                        }
                    }
                }

                val curr = getHeight(block)
                val expected = lastSeen.get() + 1
                if (curr > expected) {
                    log.debug { "current:$curr <= expected:$expected, filling the gap..." }
                    gapFill(channel, getHeight, lastSeen.get() + 1, historicalFlow)
                        .collect { collector(it) }
                } else {
                    collector(block)
                }
            }
        }
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
suspend fun <T> gapFill(
    channel: ReceiveChannel<T>,
    getHeight: (T) -> Long,
    from: Long,
    historicalFlow: (from: Long, to: Long) -> Flow<T>
) = channelFlow {
    val log = KotlinLogging.logger {}

    var firstLive = channel.receive()
    var firstLiveHeight = getHeight(firstLive)
    var lastCurrent = from

    // Determine if we have a gap between the last seen current and first received that needs to be filled.
    // If fetching and emitting the gap takes longer that the time to fetch a new block, loop and do it again
    // until the next block to be emitted is the head of the channel.
    do {
        log.debug { "gap fill from $lastCurrent to $firstLiveHeight" }
        historicalFlow(lastCurrent, firstLiveHeight)
            .collect { block ->
                // Update the lastSeen height after successful send.
                send(block)
            }
        lastCurrent = firstLiveHeight
        firstLive = channel.receive()
        firstLiveHeight = getHeight(firstLive)
    } while (lastCurrent + 1 < firstLiveHeight)

    // Send the last known head of the channel.
    send(firstLive)
}

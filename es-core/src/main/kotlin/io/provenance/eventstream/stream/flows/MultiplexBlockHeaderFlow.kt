package io.provenance.eventstream.stream.flows

import io.provenance.eventstream.decoder.DecoderAdapter
import io.provenance.eventstream.net.NetAdapter
import io.provenance.eventstream.stream.clients.BlockData
import io.provenance.eventstream.stream.models.BlockHeader
import kotlinx.coroutines.flow.Flow

/**
 * Create a [Flow] of [BlockHeader] from height to height. Uses polling under the hood for live data.
 *
 * This flow will intelligently determine how to merge the live and history flows to
 * create a seamless stream of [BlockHeader] objects.
 *
 * @param netAdapter The [NetAdapter] to use for network interfacing.
 * @param from The `from` height, if omitted, height 1 is used.
 * @param to The `to` height, if omitted, no end is assumed.
 * @param historicalFlow The historical flow data generator to use (default: [historicalBlockDataFlow])
 * @param liveFlow The live flow data generator to use (default: [pollingBlockDataFlow])
 * @return The [Flow] of [BlockData].
 */
fun blockHeaderFlow(
    netAdapter: NetAdapter,
    from: Long? = null,
    to: Long? = null,
    historicalFlow: (Long, Long) -> Flow<BlockHeader> = { f, t -> historicalBlockHeaderFlow(netAdapter, f, t) },
    liveFlow: () -> Flow<BlockHeader> = { pollingBlockHeaderFlow(netAdapter) },
): Flow<BlockHeader> = combinedFlow(currentHeightFn(netAdapter), from, to, blockHeaderHeightFn, historicalFlow, liveFlow)

/**
 * Create a [Flow] of [BlockHeader] from height to height. Uses web sockets under the hood for live data.
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
fun blockHeaderFlow(
    netAdapter: NetAdapter,
    decoderAdapter: DecoderAdapter,
    from: Long? = null,
    to: Long? = null
): Flow<BlockHeader> = blockHeaderFlow(
    netAdapter,
    from,
    to,
    historicalFlow = { f, t -> historicalBlockHeaderFlow(netAdapter, f, t) },
    liveFlow = { wsBlockHeaderFlow(netAdapter, decoderAdapter) },
)

/**
 * ----
 */

internal val blockHeaderHeightFn: (BlockHeader) -> Long = { it.height }

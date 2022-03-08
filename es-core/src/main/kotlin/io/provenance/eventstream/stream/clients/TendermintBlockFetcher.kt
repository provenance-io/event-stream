package io.provenance.eventstream.stream.clients

import io.provenance.eventstream.stream.TendermintServiceClient
import io.provenance.eventstream.stream.models.Block
import io.provenance.eventstream.stream.models.BlockMeta
import io.provenance.eventstream.stream.models.BlockResultsResponse
import tendermint.types.BlockOuterClass

class TendermintBlockFetcher(
    val tendermintServiceClient: TendermintServiceClient
) : BlockFetcher {

    override suspend fun getBlocksMeta(min: Long, max: Long): List<BlockMeta>? {
        return tendermintServiceClient.blockchain(min, max).result?.blockMetas
    }

    override suspend fun getCurrentHeight(): Long? {
        return tendermintServiceClient.abciInfo().lastBlockHeight
    }

    override suspend fun getBlock(height: Long): BlockOuterClass.Block {
        return tendermintServiceClient.block(height)
    }
    override suspend fun getBlockResults(block: BlockOuterClass.Block): io.provenance.eventstream.stream.clients.BlockResultsResponse? {
        return tendermintServiceClient.blockResults(block)
    }
}

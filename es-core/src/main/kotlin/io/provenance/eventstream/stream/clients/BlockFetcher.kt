package io.provenance.eventstream.stream.clients

import io.provenance.eventstream.stream.models.Block
import io.provenance.eventstream.stream.models.BlockMeta
import io.provenance.eventstream.stream.models.BlockResultsResponse
import tendermint.types.BlockOuterClass

interface BlockFetcher {
    suspend fun getBlocksMeta(min: Long, max: Long): List<BlockMeta>?
    suspend fun getCurrentHeight(): Long?
    suspend fun getBlock(height: Long): BlockOuterClass.Block
    suspend fun getBlockResults(height: Long): BlockResultsResponse?
}
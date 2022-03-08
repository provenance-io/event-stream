package io.provenance.eventstream.stream.models

import com.squareup.moshi.JsonClass
import cosmos.base.abci.v1beta1.Abci
import tendermint.abci.Types
import tendermint.types.BlockOuterClass
import java.io.Serializable

interface StreamBlock {
    val block: BlockOuterClass.Block
    val blockEvents: List<Types.Event>
    val txEvents: List<Abci.StringEvent>
    val historical: Boolean
    val height: Long? get() = block.header?.height
}

/**
 * Wraps a block and associated block-level and transaction-level events, as well as a marker to determine if the
 * block is "historical" (not live streamed), and metadata, if any.
 */
@JsonClass(generateAdapter = true)
open class StreamBlockImpl(
    override val block: BlockOuterClass.Block,
    override val blockEvents: List<Types.Event>,
    override val txEvents: List<Abci.StringEvent>,
    override val historical: Boolean = false
) : StreamBlock, Serializable

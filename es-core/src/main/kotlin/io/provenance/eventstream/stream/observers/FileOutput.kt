package io.provenance.eventstream.stream.observers

import io.provenance.blockchain.stream.api.BlockSink
import io.provenance.eventstream.adapter.json.decoder.Adapter
import io.provenance.eventstream.adapter.json.decoder.DecoderEngine
import io.provenance.eventstream.adapter.json.decoder.adapter
import io.provenance.eventstream.stream.models.StreamBlockImpl
import io.provenance.eventstream.stream.models.StreamBlock
import io.provenance.eventstream.utils.sha256
import java.io.File
import java.math.BigInteger

fun fileOutput(dir: String, decoder: DecoderEngine): FileOutput = FileOutput(dir, decoder)

@OptIn(ExperimentalStdlibApi::class)
class FileOutput(dir: String, decoder: DecoderEngine) : BlockSink {
    private val adapter: Adapter<StreamBlock> = decoder.adapter()
    private val dirname = { name: String -> "$dir/$name" }

    init {
        File(dir).mkdirs()
    }

    override suspend fun invoke(block: StreamBlock) {
        val checksum = sha256(block.height.toString()).toHex()
        val splay = checksum.take(4)
        val dirname = dirname(splay)

        File(dirname).let { f -> if (!f.exists()) f.mkdirs() }

        val filename = "$dirname/${block.height.toString().padStart(10, '0')}.json"
        val file = File(filename)
        if (!file.exists()) {
            file.writeText(adapter.toJson(block as StreamBlockImpl))
        }
    }
}

private fun ByteArray.toHex(): String {
    val bi = BigInteger(1, this)
    return String.format("%0" + (this.size shl 1) + "X", bi)
}

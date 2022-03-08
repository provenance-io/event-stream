package io.provenance.eventstream.stream.observers

import com.google.protobuf.util.JsonFormat
import com.squareup.moshi.JsonAdapter
import com.squareup.moshi.Moshi
import io.provenance.blockchain.stream.api.BlockSink
import io.provenance.eventstream.stream.models.StreamBlockImpl
import io.provenance.eventstream.stream.models.StreamBlock
import io.provenance.eventstream.utils.sha256
import java.io.File
import java.math.BigInteger

fun fileOutput(dir: String, decoder: Moshi): FileOutput = FileOutput(dir, decoder)

@OptIn(ExperimentalStdlibApi::class)
class FileOutput(dir: String, decoder: Moshi) : BlockSink {
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
        val blockImpl = block as StreamBlockImpl
        var fileString = JsonFormat.printer().print(blockImpl.block)
        fileString += "\n\n" + blockImpl.height
        fileString += "\n\n" + blockImpl.blockEvents.map { JsonFormat.printer().print(it) + "\n" }
        fileString += "\n\n" + blockImpl.txEvents.map { JsonFormat.printer().print(it) + "\n" }
        fileString += "\n\n" + blockImpl.historical
        if (!file.exists()) {
            file.writeText(fileString)
        }
    }
}

private fun ByteArray.toHex(): String {
    val bi = BigInteger(1, this)
    return String.format("%0" + (this.size shl 1) + "X", bi)
}

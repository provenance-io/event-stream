package io.provenance.eventstream.stream

import io.provenance.eventstream.stream.models.StreamBlockImpl
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.apache.kafka.common.errors.SerializationException
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.ObjectInputFilter
import java.io.IOException

fun Flow<KafkaStreamBlock<String, StreamBlockImpl>>.acking(block: (KafkaStreamBlock<String, StreamBlockImpl>) -> Unit): Flow<AckedKafkaStreamBlock<ByteArray, ByteArray>> {
    return flow {
        collect {
            val ackedConsumerRecordImpl = it.record.ack()
            emit(AckedKafkaStreamBlock<ByteArray, ByteArray>(ackedConsumerRecordImpl))
        }
    }
}

fun StreamBlockImpl.toByteArray(): ByteArray? {
    return if (this == null) {
        return ByteArray(0)
    } else try {
        val boas = ByteArrayOutputStream()
        ObjectOutputStream(boas).use { ois ->
            ois.writeObject(this)
            return boas.toByteArray()
        }
    } catch (e: Exception) {
        throw SerializationException(e)
    }
}

fun ByteArray.toStreamBlock(): StreamBlockImpl? {
    return if (this == null || this.size == 0) {
        null
    } else try {
        val `is`: InputStream = ByteArrayInputStream(this)
        ObjectInputStream(`is`).use { ois -> return ois.readObject() as StreamBlockImpl }
    } catch (e: Exception) {
        throw SerializationException(e)
    }
}

// Convert object to byte[]
@Throws(IOException::class)
fun convertObjectToBytes2(obj: Any?): ByteArray? {
    val boas = ByteArrayOutputStream()
    ObjectOutputStream(boas).use { ois ->
        ois.writeObject(obj)
        return boas.toByteArray()
    }
}

// Convert byte[] to object
fun convertBytesToObject(bytes: ByteArray?): Any? {
    val `is`: InputStream = ByteArrayInputStream(bytes)
    try {
        ObjectInputStream(`is`).use { ois -> return ois.readObject() }
    } catch (ioe: IOException) {
        ioe.printStackTrace()
    } catch (ioe: ClassNotFoundException) {
        ioe.printStackTrace()
    }
    throw RuntimeException()
}

// Convert byte[] to object
@Throws(IOException::class, ClassNotFoundException::class)
fun convertBytesToObject2(bytes: ByteArray?): Any? {
    val `is`: InputStream = ByteArrayInputStream(bytes)
    ObjectInputStream(`is`).use { ois -> return ois.readObject() }
}

// Convert byte[] to object with filter
fun convertBytesToObjectWithFilter(bytes: ByteArray?, filter: ObjectInputFilter?): StreamBlockImpl? {
    val `is`: InputStream = ByteArrayInputStream(bytes)
    try {
        ObjectInputStream(`is`).use { ois ->

            // add filter before readObject
            ois.objectInputFilter = filter
            return ois.readObject() as StreamBlockImpl
        }
    } catch (ioe: IOException) {
        ioe.printStackTrace()
    } catch (ioe: ClassNotFoundException) {
        ioe.printStackTrace()
    }
    throw RuntimeException()
}

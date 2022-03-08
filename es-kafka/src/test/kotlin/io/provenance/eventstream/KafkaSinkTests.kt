package io.provenance.eventstream

import io.provenance.eventstream.stream.kafkaBlockSink
import io.provenance.eventstream.stream.models.BlockEvent
import io.provenance.eventstream.stream.models.BlockResultsResponse
import io.provenance.eventstream.stream.models.StreamBlockImpl
import io.provenance.eventstream.stream.toByteArray
import io.provenance.eventstream.test.base.TestBase
import io.provenance.eventstream.test.utils.Defaults
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import tendermint.types.BlockOuterClass
import java.lang.RuntimeException
import java.time.OffsetDateTime

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaSinkTests : TestBase() {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testKafkaSinkSendSuccess() = dispatcherProvider.runBlockingTest {

        val serializer = Serdes.ByteArray().serializer()

        val producerProps = mapOf(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            CommonClientConfigs.CLIENT_ID_CONFIG to ("test0"),
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to serializer.javaClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to serializer.javaClass,
        )

        val mockProducer: MockProducer<ByteArray, ByteArray> =
            MockProducer(false, serializer, serializer)

        val templates = Defaults.templates
        val blockResponse = templates.readAs(BlockOuterClass.Block.newBuilder(), "block/2270370.json")
        val blockResultsResponse = templates.readAs(BlockResultsResponse::class.java, "block_results/2270370.json")

        val blockEvents = blockResultsResponse!!.result.beginBlockEvents!!.map {
            BlockEvent(blockResultsResponse.result.height, OffsetDateTime.now(), it.type!!, it.attributes!!)
        }
        val streamBlock = StreamBlockImpl(blockResponse!!, blockEvents, mutableListOf())
        assert(mockProducer.history().isEmpty())

        val expectedKey =
            "${blockResponse!!.header!!.chainId}.${blockResponse.header!!.height}"

        val record = kafkaBlockSink(producerProps, "testTopic", mockProducer).kafkaSink.sendHelper(
            expectedKey.toByteArray(),
            streamBlock.toByteArray()!!
        )

        mockProducer.completeNext()

        record.get()

        assertEquals(mockProducer.history().size, 1)
        assertEquals(mockProducer.history()[0].key().decodeToString(), expectedKey)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testKafkaSinkSendFail() = dispatcherProvider.runBlockingTest {

        val serializer = Serdes.ByteArray().serializer()
        val producerProps = mapOf(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            CommonClientConfigs.CLIENT_ID_CONFIG to ("test0"),
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to serializer.javaClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to serializer.javaClass,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to serializer.javaClass,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to serializer.javaClass,
        )

        val mockProducer: MockProducer<ByteArray, ByteArray> =
            MockProducer(false, serializer, serializer)

        val templates = Defaults.templates
        val blockResponse = templates.readAs(BlockOuterClass.Block.newBuilder(), "block/2270370.json")
        val blockResultsResponse = templates.readAs(BlockResultsResponse::class.java, "block_results/2270370.json")

        val blockEvents = blockResultsResponse!!.result.beginBlockEvents!!.map {
            BlockEvent(blockResultsResponse.result.height, OffsetDateTime.now(), it.type!!, it.attributes!!)
        }
        val streamBlock = StreamBlockImpl(blockResponse!!, blockEvents, mutableListOf())

        val expectedKey =
            "${blockResponse.header!!.chainId}.${blockResponse.header!!.height}"

        val record = kafkaBlockSink(producerProps, "testTopic", mockProducer).kafkaSink.sendHelper(
            streamBlock.toByteArray()!!,
            expectedKey.toByteArray()
        )

        val e = RuntimeException()
        mockProducer.errorNext(e)

        try {
            record.get()
        } catch (ex: Exception) {
            assertEquals(e, ex.cause)
        }
    }
}

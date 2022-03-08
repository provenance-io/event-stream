package io.provenance.eventstream

import com.google.protobuf.util.JsonFormat
import com.squareup.moshi.JsonAdapter
import io.provenance.eventstream.stream.KafkaStreamBlock
import io.provenance.eventstream.stream.acking
import io.provenance.eventstream.stream.models.BlockEvent
import io.provenance.eventstream.stream.models.BlockResultsResponse
import io.provenance.eventstream.stream.models.StreamBlockImpl
import io.provenance.eventstream.stream.toByteArray
import io.provenance.eventstream.stream.toStreamBlock
import io.provenance.eventstream.test.base.TestBase
import io.provenance.kafka.coroutine.kafkaChannel
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.SerializationException
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import tendermint.types.BlockOuterClass
import java.lang.IllegalStateException
import java.time.OffsetDateTime
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaSourceTests : TestBase() {

    private val consumerProps = mapOf(
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 1,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 10000,
        ConsumerConfig.GROUP_ID_CONFIG to "test-group",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        CommonClientConfigs.CLIENT_ID_CONFIG to (UUID.randomUUID().toString()),
    )
    private var streamBlocks = mutableMapOf<String, StreamBlockImpl>()

    @BeforeAll
    override fun setup() {
        val blockResponses = mutableMapOf<String, BlockOuterClass.Block>()
        val blockResultsResponses = mutableMapOf<String, BlockResultsResponse>()
        templates.readAll("block").forEach {
            var blockBuilder = BlockOuterClass.Block.newBuilder()
            JsonFormat.parser().ignoringUnknownFields().merge(it, blockBuilder)
            val blockResponse = blockBuilder.build()
            blockResponses[blockResponse!!!!.header!!.height.toString()] = blockResponse
        }
        templates.readAll("block_results").forEach {
            val adapter: JsonAdapter<BlockResultsResponse> = moshi.adapter(BlockResultsResponse::class.java)
            val blockResultsResponse = adapter.fromJson(it)
            blockResultsResponses[blockResultsResponse!!.result.height.toString()] = blockResultsResponse
        }
        blockResultsResponses.forEach { k, v ->
            val blockEvents = v.result.beginBlockEvents!!.map {
                BlockEvent(v.result.height, OffsetDateTime.now(), it.type!!, it.attributes!!)
            }
            streamBlocks[k] = StreamBlockImpl(blockResponses[k]!!, blockEvents, mutableListOf())
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testStreamBlockByteArrayExtensions() {
        val streamBytes = streamBlocks["2270370"]!!.toByteArray()
        val streamBlockImpl = streamBytes!!.toStreamBlock()
        assert(streamBlockImpl!!.height == 2270370L)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testStreamBlockByteArrayExtensionsEmpty() {
        val streamBytes = StreamBlockImpl(BlockOuterClass.Block.getDefaultInstance(), mutableListOf(), mutableListOf()).toByteArray()
        val streamBlockImpl = streamBytes!!.toStreamBlock()
        assert(streamBlockImpl!!.block.header.chainId == "")
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testStreamBlockByteArrayExtensionsIncompatibleValue() {
        val streamBytes = "failStrin".toByteArray()
        assertThrows<SerializationException> {
            streamBytes.toStreamBlock()
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testKafkaConsumerMultipleRecords() {
        val mockConsumer = MockConsumer<ByteArray, ByteArray>(
            OffsetResetStrategy.EARLIEST
        )
        val tp1 = TopicPartition("test-topic", 0)
        mockConsumer.schedulePollTask {
            mockConsumer.rebalance(listOf(tp1))
        }
        mockConsumer.schedulePollTask {
            mockConsumer.addRecord(
                ConsumerRecord(
                    "test-topic",
                    0,
                    0L,
                    "testing1".toByteArray(),
                    streamBlocks["2270370"]!!.toByteArray()
                )
            )
            mockConsumer.addRecord(
                ConsumerRecord(
                    "test-topic",
                    0,
                    1L,
                    "testing2".toByteArray(),
                    streamBlocks["2270371"]!!.toByteArray()
                )
            )
            mockConsumer.addRecord(
                ConsumerRecord(
                    "test-topic",
                    0,
                    3L,
                    "testing3".toByteArray(),
                    streamBlocks["2270372"]!!.toByteArray()
                )
            )
        }
        val startOffsets: HashMap<TopicPartition, Long> = HashMap()

        startOffsets[tp1] = 0L
        mockConsumer.updateBeginningOffsets(startOffsets)
        val results = mutableListOf<KafkaStreamBlock<String, StreamBlockImpl>>()
        try {
            runBlocking {
                val kafkaChannel = kafkaChannel<ByteArray, ByteArray>(
                    consumerProps,
                    setOf("test-topic"),
                    consumer = mockConsumer
                )
                mockConsumer.scheduleNopPollTask()
                mockConsumer.schedulePollTask {
                    kafkaChannel.cancel()
                }
                kafkaChannel.receiveAsFlow().map { KafkaStreamBlock<String, StreamBlockImpl>(it) }
                    .onEach {
                        results.add(it)
                    }
                    .acking {}.collect {}
            }
        } catch (ex: Exception) {
            assert(results.size == 3)
            assert(results[0].height == 2270370L)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testKafkaConsumerEmptyPoll() {
        val mockConsumer = MockConsumer<ByteArray, ByteArray>(
            OffsetResetStrategy.EARLIEST
        )
        val tp1 = TopicPartition("test-topic", 0)
        mockConsumer.schedulePollTask {
            mockConsumer.rebalance(listOf(tp1))
        }
        mockConsumer.scheduleNopPollTask()
        mockConsumer.schedulePollTask {
            mockConsumer.addRecord(
                ConsumerRecord(
                    "test-topic",
                    0,
                    0L,
                    "testing1".toByteArray(),
                    streamBlocks["2270370"]!!.toByteArray()
                )
            )
        }
        val startOffsets: HashMap<TopicPartition, Long> = HashMap()

        startOffsets[tp1] = 0L
        mockConsumer.updateBeginningOffsets(startOffsets)
        val results = mutableListOf<KafkaStreamBlock<String, StreamBlockImpl>>()
        try {
            runBlocking {
                val kafkaChannel = kafkaChannel<ByteArray, ByteArray>(
                    consumerProps,
                    setOf("test-topic"),
                    consumer = mockConsumer
                )
                mockConsumer.scheduleNopPollTask()
                mockConsumer.schedulePollTask {
                    mockConsumer.close()
                }
                kafkaChannel.receiveAsFlow().map { KafkaStreamBlock<String, StreamBlockImpl>(it) }
                    .onEach {
                        results.add(it)
                    }
                    .acking {}.collect {}
            }
        } catch (ex: Exception) {
            assert(results.size == 1)
            assert(results[0].height == 2270370L)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testKafkaConsumerClosedError() {
        val mockConsumer = MockConsumer<ByteArray, ByteArray>(
            OffsetResetStrategy.EARLIEST
        )
        val tp1 = TopicPartition("test-topic", 0)
        val startOffsets: HashMap<TopicPartition, Long> = HashMap()

        startOffsets[tp1] = 0L
        mockConsumer.updateBeginningOffsets(startOffsets)
        val results = mutableListOf<KafkaStreamBlock<String, StreamBlockImpl>>()
        assertThrows<IllegalStateException> {
            runBlocking {
                val kafkaChannel = kafkaChannel<ByteArray, ByteArray>(
                    consumerProps,
                    setOf("test-topic"),
                    consumer = mockConsumer
                )
                mockConsumer.schedulePollTask {
                    mockConsumer.close()
                }
                kafkaChannel.receiveAsFlow().map { KafkaStreamBlock<String, StreamBlockImpl>(it) }
                    .onEach {
                        results.add(it)
                    }
                    .acking {}.collect {}
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testKafkaConsumerWrongBytes() {
        val mockConsumer = MockConsumer<ByteArray, ByteArray>(
            OffsetResetStrategy.EARLIEST
        )
        val tp1 = TopicPartition("test-topic", 0)
        mockConsumer.schedulePollTask {
            mockConsumer.rebalance(listOf(tp1))
        }
        mockConsumer.schedulePollTask {
            mockConsumer.addRecord(
                ConsumerRecord(
                    "test-topic",
                    0,
                    0L,
                    "testing1".toByteArray(),
                    "Wrong value for testing".toByteArray()
                )
            )
        }
        val startOffsets: HashMap<TopicPartition, Long> = HashMap()

        startOffsets[tp1] = 0L
        mockConsumer.updateBeginningOffsets(startOffsets)
        val results = mutableListOf<KafkaStreamBlock<String, StreamBlockImpl>>()
        assertThrows<IllegalStateException> {
            runBlocking {
                val kafkaChannel = kafkaChannel<ByteArray, ByteArray>(
                    consumerProps,
                    setOf("test-topic"),
                    consumer = mockConsumer
                )
                mockConsumer.schedulePollTask {
                    mockConsumer.close()
                }
                kafkaChannel.receiveAsFlow().map { KafkaStreamBlock<String, StreamBlockImpl>(it) }
                    .onEach {
                        results.add(it)
                    }
                    .acking {}.collect {}
            }
        }
    }
}

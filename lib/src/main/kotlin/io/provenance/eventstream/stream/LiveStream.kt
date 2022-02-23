package io.provenance.eventstream.stream

import com.squareup.moshi.JsonDataException
import com.squareup.moshi.Moshi
import com.tinder.scarlet.Message
import com.tinder.scarlet.WebSocket
import io.provenance.blockchain.stream.api.BlockSource
import io.provenance.eventstream.config.Options
import io.provenance.eventstream.coroutines.DefaultDispatcherProvider
import io.provenance.eventstream.coroutines.DispatcherProvider
import io.provenance.eventstream.stream.models.Block
import io.provenance.eventstream.stream.models.StreamBlockImpl
import io.provenance.eventstream.stream.models.rpc.request.Subscribe
import io.provenance.eventstream.stream.models.rpc.response.MessageType
import io.provenance.eventstream.stream.transformers.queryBlock
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.retryWhen
import mu.KotlinLogging

class LiveStream(
    private val eventStreamService: EventStreamService,
    private val tendermintServiceClient: TendermintServiceClient,
    private val moshi: Moshi,
    private val dispatchers: DispatcherProvider = DefaultDispatcherProvider(),
    private val options: Options = Options.DEFAULT
) : BlockSource<StreamBlockImpl> {

    private val log = KotlinLogging.logger { }

    /**
     *
     * Constructs a Flow of newly minted blocks and associated events as the blocks are added to the chain.
     *
     * @return A Flow of newly minted blocks and associated events
     */
    fun streamLiveBlocks(): Flow<StreamBlockImpl> {

        // Toggle the Lifecycle register start state:
        eventStreamService.startListening()

        return channelFlow {
            for (event in eventStreamService.observeWebSocketEvent()) {
                when (event) {
                    is WebSocket.Event.OnConnectionOpened<*> -> {
                        log.info("streamLiveBlocks::received OnConnectionOpened event")
                        log.info("streamLiveBlocks::initializing subscription for tm.event='NewBlock'")
                        eventStreamService.subscribe(Subscribe("tm.event='NewBlock'"))
                    }
                    is WebSocket.Event.OnMessageReceived -> when (event.message) {
                        is Message.Text -> {
                            val message = event.message as Message.Text
                            when (val type = responseMessageDecoder.decode(message.value)) {
                                is MessageType.Empty -> log.info("received empty ACK message => ${message.value}")
                                is MessageType.NewBlock -> {
                                    val block = type.block.data.value.block
                                    log.info("live::received NewBlock message: #${block.header?.height}")
                                    send(block)
                                }
                                is MessageType.Error -> log.error("upstream error from RPC endpoint: ${type.error}")
                                is MessageType.Panic -> {
                                    log.error("upstream panic from RPC endpoint: ${type.error}")
                                    throw CancellationException("RPC endpoint panic: ${type.error}")
                                }
                                is MessageType.Unknown -> log.info("unknown message type; skipping message => ${message.value}")
                            }
                        }
                        is Message.Bytes -> {
                            // ignore; binary payloads not supported:
                            log.warn("live::binary message payload not supported")
                        }
                    }
                    is WebSocket.Event.OnConnectionFailed -> throw event.throwable
                    else -> throw Throwable("live::unexpected event type: $event")
                }
            }
        }.flowOn(dispatchers.io()).onStart { log.info("live::starting") }.mapNotNull { block: Block ->
            val maybeBlock = queryBlock(
                block.header!!.height,
                skipIfNoTxs = false,
                historical = false,
                tendermintServiceClient,
                options
            )
            if (maybeBlock != null) {
                log.info("live::got block #${maybeBlock.height}")
                maybeBlock
            } else {
                log.info("live::skipping block #${block.header?.height}")
                null
            }
        }.onCompletion {
            log.info("live::stopping event stream")
            eventStreamService.stopListening()
        }.retryWhen { cause: Throwable, attempt: Long ->
            log.warn("live::error; recovering Flow (attempt ${attempt + 1})")
            when (cause) {
                is JsonDataException -> {
                    log.error("streamLiveBlocks::parse error, skipping: $cause")
                    true
                }
                else -> false
            }
        }
    }

    /**
     * A decoder for Tendermint RPC API messages.
     */
    private val responseMessageDecoder: MessageType.Decoder = MessageType.Decoder(moshi)

    override fun streamBlocks(): Flow<StreamBlockImpl> = streamLiveBlocks()
}

package io.provenance.eventstream.stream.clients

import cosmos.base.abci.v1beta1.Abci
import cosmos.base.tendermint.v1beta1.Query
import cosmos.base.tendermint.v1beta1.ServiceGrpc
import cosmos.tx.v1beta1.ServiceOuterClass
import io.grpc.ManagedChannelBuilder
import io.provenance.client.grpc.GasEstimationMethod
import io.provenance.client.grpc.PbClient
import io.provenance.eventstream.extensions.txHash
import io.provenance.eventstream.stream.TendermintServiceClient
import io.provenance.eventstream.stream.apis.InfoApi
import io.provenance.eventstream.stream.models.BlockResultsResponse
import io.provenance.eventstream.stream.models.BlockchainResponse
import io.provenance.eventstream.stream.models.extensions.hash
import tendermint.abci.ABCIApplicationGrpc
import tendermint.abci.Types
import tendermint.types.BlockOuterClass
import java.net.URI

/**
 * An OpenAPI generated client designed to interact with the Tendermint RPC API.
 *
 * All requests and responses are HTTP+JSON.
 *
 * @param rpcUrlBase The base URL of the Tendermint RPC API to use when making requests.
 */
class TendermintServiceOpenApiClient(rpcUrlBase: URI, chainId: String) : TendermintServiceClient {
    private val pbClient = let {
        var port = rpcUrlBase.port
        if (rpcUrlBase.host in mutableListOf("localhost", "127.0.0.1", "0.0.0.0")) {
            port = 9090
        }
        val uri = URI(rpcUrlBase.scheme + "://" + rpcUrlBase.host + ":" + port)

        PbClient(
            chainId = chainId,
            channelUri = uri,
            gasEstimationMethod = GasEstimationMethod.MSG_FEE_CALCULATION
        )
    }

    private val abciApiInfo = ABCIApplicationGrpc.newFutureStub(
        ManagedChannelBuilder
            .forAddress(rpcUrlBase.host, rpcUrlBase.port)
            .useTransportSecurity()
            .build()
    )

    private val abciApi = let {
        var port = rpcUrlBase.port
        if (rpcUrlBase.host in mutableListOf("localhost", "127.0.0.1", "0.0.0.0")) {
            port = 9090
        }

        ABCIApplicationGrpc.newFutureStub(
            ManagedChannelBuilder
                .forAddress(rpcUrlBase.host, 9090)
                .usePlaintext()
                .build()
        )
    }

    private val infoApi = let {
        InfoApi("http" + "://" + rpcUrlBase.host + ":" + rpcUrlBase.port)
    }

    override suspend fun abciInfo(): Types.ResponseInfo = abciApiInfo.info(Types.RequestInfo.getDefaultInstance()).get()

    override suspend fun block(height: Long?): BlockOuterClass.Block =
        pbClient.tendermintService.getBlockByHeight(
            Query.GetBlockByHeightRequest.newBuilder().setHeight(height!!).build()
        ).block

//    override suspend fun blockResults(height: Long?): BlockResultsResponse = infoApi.blockResults(height)
    override suspend fun blockResults(block: BlockOuterClass.Block): io.provenance.eventstream.stream.clients.BlockResultsResponse? {
        var txs = block.data.txsList
        var blockResponse = abciApi.beginBlock(Types.RequestBeginBlock.newBuilder().setHeader(block.header).build()).get()

        var txResponse = txs.map {
            pbClient.cosmosService.getTx(ServiceOuterClass.GetTxRequest.newBuilder().setHash(it.hash()).build())
        }

        var txEvents = txResponse.flatMap { it.txResponse.logsList.map { it.eventsList[0] } }
        var blockEvents = (blockResponse as Types.ResponseEndBlock).eventsList

        return BlockResultsResponse(txEvents, blockEvents)
    }

    override suspend fun blockchain(minHeight: Long?, maxHeight: Long?): BlockchainResponse =
        infoApi.blockchain(minHeight, maxHeight)
}

data class BlockResultsResponse(val txEvents: List<Abci.StringEvent>, val blockEvents: List<Types.Event>)

package io.provenance.eventstream.test.mocks

import io.provenance.eventstream.stream.TendermintServiceClient
import io.provenance.eventstream.stream.models.BlockResultsResponse
import io.provenance.eventstream.stream.models.BlockchainResponse
import tendermint.abci.Types
import tendermint.types.BlockOuterClass.Block

class MockTendermintServiceClient(mocker: ServiceMock) : TendermintServiceClient, ServiceMock by mocker {

    override suspend fun abciInfo() =
        respondWith<Types.ResponseInfo>("abciInfo")

    override suspend fun block(height: Long?) =
        respondWith<Block>("block", height)

    override suspend fun blockResults(height: Long?) =
        respondWith<BlockResultsResponse>("blockResults", height)

    override suspend fun blockchain(minHeight: Long?, maxHeight: Long?) =
        respondWith<BlockchainResponse>("blockchain", minHeight, maxHeight)
}

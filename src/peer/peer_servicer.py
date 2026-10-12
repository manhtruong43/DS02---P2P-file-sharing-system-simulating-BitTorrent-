import logging
import grpc

try:
    from src.common import peer_pb2, peer_pb2_grpc
except ImportError:
    try:
        import peer_pb2
        import peer_pb2_grpc
    except ImportError:
        from common import peer_pb2, peer_pb2_grpc

logger = logging.getLogger("peer_servicer")

class PeerServicer(peer_pb2_grpc.PeerServiceServicer):
    """
    gRPC Server handling chunk requests, handshakes, and bitfield exchanges from peer nodes.
    """
    def __init__(self, node_context):
        self.node = node_context

    async def Handshake(self, request: peer_pb2.HandshakeRequest, context: grpc.aio.ServicerContext) -> peer_pb2.HandshakeResponse:
        logger.info(f"Handshake received from peer {request.peer_id} for file {request.file_id[:8]}..")
        if request.file_id != self.node.file_id:
            return peer_pb2.HandshakeResponse(
                peer_id=self.node.peer_id,
                success=False,
                message=f"File ID mismatch: {request.file_id} != {self.node.file_id}"
            )
        return peer_pb2.HandshakeResponse(
            peer_id=self.node.peer_id,
            success=True,
            message="Handshake successful"
        )

    async def GetBitfield(self, request: peer_pb2.BitfieldRequest, context: grpc.aio.ServicerContext) -> peer_pb2.BitfieldResponse:
        if request.file_id != self.node.file_id:
            return peer_pb2.BitfieldResponse(file_id=request.file_id, chunks_owned=[])
        
        owned_list = sorted(list(self.node.chunk_mgr.chunks_owned))
        return peer_pb2.BitfieldResponse(
            file_id=self.node.file_id,
            chunks_owned=owned_list
        )

    async def GetChunk(self, request: peer_pb2.ChunkRequest, context: grpc.aio.ServicerContext) -> peer_pb2.ChunkResponse:
        file_id = request.file_id
        chunk_index = request.chunk_index

        if file_id != self.node.file_id:
            return peer_pb2.ChunkResponse(
                success=False,
                chunk_index=chunk_index,
                error_message="File not managed by this peer node"
            )

        if chunk_index not in self.node.chunk_mgr.chunks_owned:
            return peer_pb2.ChunkResponse(
                success=False,
                chunk_index=chunk_index,
                error_message=f"Chunk {chunk_index} not currently available"
            )

        try:
            chunk_data = self.node.chunk_mgr.read_chunk(chunk_index)
            logger.debug(f"Serving chunk {chunk_index} ({len(chunk_data)} bytes) to requesting peer")
            return peer_pb2.ChunkResponse(
                success=True,
                data=chunk_data,
                chunk_index=chunk_index,
                error_message=""
            )
        except Exception as e:
            logger.error(f"Failed to serve chunk {chunk_index}: {e}")
            return peer_pb2.ChunkResponse(
                success=False,
                chunk_index=chunk_index,
                error_message=str(e)
            )

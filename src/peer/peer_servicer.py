import os
import asyncio
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


class PeerServicer(peer_pb2_grpc.PeerServiceServicer):
    """
    gRPC Server handling chunk requests, handshakes, and bitfield exchanges from peer nodes.
    """
    def __init__(self, node_context):
        self.node = node_context
        label = getattr(node_context, "peer_id", "")
        self.log = logging.getLogger(f"servicer[{label}]" if label else "peer_servicer")
        # Bandwidth limiting: a single upload link, chunks are sent one after another
        self.uplink = asyncio.Lock()

    async def Handshake(self, request: peer_pb2.HandshakeRequest, context: grpc.aio.ServicerContext) -> peer_pb2.HandshakeResponse:
        self.log.info(f"[HANDSHAKE] From peer {request.peer_id} for file {request.file_id[:8]}..")
        if request.file_id != self.node.file_id:
            self.log.warning(f"[HANDSHAKE] Rejected peer {request.peer_id}: unknown file_id {request.file_id[:8]}..")
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
        requester = context.peer() if context is not None else "unknown"

        if file_id != self.node.file_id:
            self.log.warning(f"[REJECT] Chunk request from {requester} for unknown file {file_id[:8]}..")
            return peer_pb2.ChunkResponse(
                success=False,
                chunk_index=chunk_index,
                error_message="File not managed by this peer node"
            )

        if chunk_index not in self.node.chunk_mgr.chunks_owned:
            self.log.warning(f"[REJECT] Chunk {chunk_index} requested by {requester} is not available here")
            return peer_pb2.ChunkResponse(
                success=False,
                chunk_index=chunk_index,
                error_message=f"Chunk {chunk_index} not currently available"
            )

        try:
            chunk_data = self.node.chunk_mgr.read_chunk(chunk_index)

            owned = len(self.node.chunk_mgr.chunks_owned)
            total = self.node.chunk_mgr.total_chunks
            progress = f"own {owned}/{total} chunks"
            if owned < total:
                progress += " - STILL DOWNLOADING, uploading what I already have"

            # Fault injection (demo only): a malicious / faulty peer sends wrong bytes
            if getattr(self.node, "corrupt", False):
                chunk_data = os.urandom(len(chunk_data))
                self.log.warning(
                    f"[FAULT INJECTION] Sending CORRUPTED data for chunk {chunk_index} to {requester}"
                )
            else:
                self.log.info(
                    f"[UPLOAD] Serving chunk {chunk_index} ({len(chunk_data)} B) to {requester} | {progress}"
                )

            # Upload bandwidth limit (bytes/s): sending a chunk occupies the upload link len/rate seconds
            rate = getattr(self.node, "upload_limit_bps", 0)
            if rate > 0:
                async with self.uplink:
                    await asyncio.sleep(len(chunk_data) / rate)

            return peer_pb2.ChunkResponse(
                success=True,
                data=chunk_data,
                chunk_index=chunk_index,
                error_message=""
            )
        except Exception as e:
            self.log.error(f"Failed to serve chunk {chunk_index}: {e}")
            return peer_pb2.ChunkResponse(
                success=False,
                chunk_index=chunk_index,
                error_message=str(e)
            )

#include <FileClient/PacketSender.h>

namespace FileClient {

PacketSender::PacketSender(Connection& connection) noexcept : connection_(connection) {}

bool PacketSender::SendPacket(
    HQUIC stream,
    PacketType type,
    const void* payload,
    std::uint32_t payloadSize,
    std::uint32_t requestId) {
    return connection_.SendPacketImpl(stream, type, payload, payloadSize, requestId);
}

bool PacketSender::SendChunk(
    HQUIC stream,
    const ChunkFileHeader& chunk,
    const std::vector<std::uint8_t>& data) {
    return connection_.SendChunkImpl(stream, chunk, data);
}

}

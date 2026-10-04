#pragma once

#include <FileClient/Connection.h>

namespace FileClient {

class PacketSender {
public:
    explicit PacketSender(Connection& connection) noexcept;

    bool SendPacket(HQUIC stream, PacketType type, const void* payload,
                    std::uint32_t payloadSize, std::uint32_t requestId = 0);
    bool SendChunk(HQUIC stream, const ChunkFileHeader& chunk,
                   const std::vector<std::uint8_t>& data);
    bool ChunkSend(HQUIC stream, ChunkFileHeader& chunk,
                   const std::vector<std::uint8_t>& data) {
        return SendChunk(stream, chunk, data);
    }

private:
    Connection& connection_;
};

} // namespace FileClient

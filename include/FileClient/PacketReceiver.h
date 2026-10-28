#pragma once

#include <filesystem>
#include <vector>

#include <FileClient/Chunk.h>
#include <FileClient/FileInfo.h>

namespace FileClient {

class Connection;

// Один экземпляр на поток (StreamContext::ReceiverManager). Буфер принадлежит потоку и
// используется только из его колбэка. MsQuic вызывает колбэки одного соединения
// последовательно, поэтому мьютекс на буфер не нужен. Не трогать buffer_ из других потоков.
class PacketReceiver {
public:
    explicit PacketReceiver(Connection& connection);

    void Receiver(const std::uint8_t* data, std::uint32_t size);
    void Finish();

private:
    struct ReceivedPacket {
        PacketHeader header;
        std::vector<std::uint8_t> payload;
    };

    void PacketDispatcher(ReceivedPacket packet);

    bool ValidPacket(const Packet& packet) const;
    bool ReceiverString(const Packet& packet, std::string& value) const;
    bool ReceiverUploadState(const Packet& packet, std::uint64_t& fileId, UploadState& state,
                            TransferDirection& direction) const;
    bool SetStateDownload(std::uint64_t fileId, UploadState state);
    bool PacketFileState(const Packet& packet);
    bool ReceiverPacketToFileInfo(const Packet& packet, FileInfo& fileInfo) const;
    bool ReceiverPacketToChunk(const Packet& packet, ChunkFileHeader& chunk,
                                const std::uint8_t*& data) const;
    bool PacketFileInfo(const Packet& packet);
    bool PacketChunk(const Packet& packet);
    void Report(const std::string& message) const;

    Connection& connection_;
    std::vector<std::uint8_t> buffer_;
};

} // namespace FileClient

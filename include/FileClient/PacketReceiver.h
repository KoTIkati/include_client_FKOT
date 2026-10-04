#pragma once

#include <filesystem>
#include <fstream>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <FileClient/FileInfo.h>

namespace FileClient {

class Connection;

class FileReceiver {
public:
    ChunkFileInfo fileInfo;
    std::ofstream file;
    std::vector<bool> received;
    std::uint32_t receivedCount = 0;
    fs::path temporaryPath;

    bool ReceiverChunk(const ChunkFileHeader& chunk, const std::uint8_t* data);
    bool ValidFileInfo() const;
};

class PacketReceiver {
public:
    explicit PacketReceiver(Connection& connection);

    void Receiver(const std::uint8_t* data, std::uint32_t size);
    void Finish();
    void SetDownloadDirectory(const fs::path& directory);

private:
    bool ValidPacket(const Packet& packet) const;
    bool ReceiverString(const Packet& packet, std::string& value) const;
    bool ReceiverPacketToFileInfo(const Packet& packet, FileInfo& fileInfo) const;
    bool ReceiverPacketToChunk(const Packet& packet, ChunkFileHeader& chunk,
                               const std::uint8_t*& data) const;
    bool PacketFileInfo(const Packet& packet);
    bool PacketChunk(const Packet& packet);
    void PacketDispatcher(const Packet& packet);
    void Report(const std::string& message) const;

    Connection& connection_;
    fs::path downloadDirectory_ = ".";
    std::vector<std::uint8_t> buffer_;
    std::unordered_map<std::uint64_t, FileReceiver> sessions_;
    std::mutex mutex_;
};

} // namespace FileClient

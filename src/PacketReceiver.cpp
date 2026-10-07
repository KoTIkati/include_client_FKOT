#include <FileClient/PacketReceiver.h>

#include <cstring>
#include <system_error>

#include <FileClient/Connection.h>

namespace FileClient {

namespace {

bool IsSafeRelativePath(const fs::path& path) {
    if (path.empty() || path.is_absolute() || path.has_root_name() || path.has_root_directory()) {
        return false;
    }
    for (const auto& element : path) {
        if (element == "..") {
            return false;
        }
    }
    return true;
}

} // namespace

bool FileReceiver::ReceiverChunk(const ChunkFileHeader& chunk, const std::uint8_t* data) {
    if (data == nullptr || !file.is_open() || chunk.Index >= fileInfo.ChunkCount ||
        chunk.Size == 0 || chunk.Size > fileInfo.ChunkSize) {
        return false;
    }
    const auto offset = static_cast<std::uint64_t>(chunk.Index) * fileInfo.ChunkSize;
    if (offset > fileInfo.Size || chunk.Size > fileInfo.Size - offset) {
        return false;
    }
    file.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    file.write(reinterpret_cast<const char*>(data), chunk.Size);
    if (!file) {
        return false;
    }
    if (!received[chunk.Index]) {
        received[chunk.Index] = true;
        ++receivedCount;
    }
    return true;
}

bool FileReceiver::ValidFileInfo() const {
    return fileInfo.FileId != 0 && fileInfo.Size != 0 && fileInfo.ChunkSize != 0 &&
           fileInfo.ChunkCount != 0 && !fileInfo.Name.empty() && !fileInfo.Path.empty();
}

PacketReceiver::PacketReceiver(Connection& connection) : connection_(connection) {}

void PacketReceiver::SetDownloadDirectory(const fs::path& directory) {
    std::lock_guard<std::mutex> lock(mutex_);
    downloadDirectory_ = directory;
}

void PacketReceiver::Report(const std::string& message) const {
    connection_.reporter().Report(message);
}

bool PacketReceiver::ValidPacket(const Packet& packet) const {
    if (packet.header.Version != PROTOCOL_VERSION) {
        Report("[ERROR] Protocol version mismatch");
        return false;
    }
    return true;
}

bool PacketReceiver::ReceiverString(const Packet& packet, std::string& value) const {
    value.assign(reinterpret_cast<const char*>(packet.payload), packet.payloadSize);
    if (value.empty()) {
        Report("[ERROR] Received an empty message");
        return false;
    }
    return true;
}

void PacketReceiver::Receiver(const std::uint8_t* data, std::uint32_t size) {
    if (data == nullptr || size == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    buffer_.insert(buffer_.end(), data, data + size);
    while (buffer_.size() >= sizeof(PacketHeader)) {
        PacketHeader header{};
        std::memcpy(&header, buffer_.data(), sizeof(header));
        if (header.Length > MAX_PACKET_SIZE) {
            Report("[ERROR] Received packet exceeds the protocol limit");
            buffer_.clear();
            return;
        }
        const auto totalSize = sizeof(PacketHeader) + static_cast<std::size_t>(header.Length);
        if (buffer_.size() < totalSize) {
            return;
        }
        Packet packet{header, buffer_.data() + sizeof(PacketHeader), header.Length};
        if (!ValidPacket(packet)) {
            buffer_.clear();
            return;
        }
        PacketDispatcher(packet);
        buffer_.erase(buffer_.begin(), buffer_.begin() + totalSize);
    }
}
std::filesystem::path Utf8ToPath(const std::string& utf8){ //#ff0000 тестовый вариант пока нет тестов на линукс системе 
    #ifdef _WIN32
        return std::filesystem::path(utf8);
    #else
        return std::filesystem::path(utf8);
    #endif
}

bool PacketReceiver::ReceiverPacketToFileInfo(const Packet& packet, FileInfo& fileInfo) const {
    if (packet.payloadSize < sizeof(FileInfoSendData)) {
        return false;
    }
    FileInfoSendData base{};
    std::memcpy(&base, packet.payload, sizeof(base));
    const std::uint8_t* data = packet.payload + sizeof(base);
    std::size_t metadataSize = sizeof(base);
    std::uint32_t nameLength = base.NameLength;
    std::uint32_t pathLength = base.PathLength;

    switch (base.type) {
    case FileInfoType::Manager: {
        if (packet.payloadSize < sizeof(ManagerFileInfoSendData)) {
            return false;
        }
        ManagerFileInfoSendData metadata{};
        std::memcpy(&metadata, packet.payload, sizeof(metadata));
        metadataSize = sizeof(metadata);
        nameLength = metadata.NameLength;
        pathLength = metadata.PathLength;
        break;
    }
    case FileInfoType::Chunk: {
        if (packet.payloadSize < sizeof(ChunkFileInfoSendData)) {
            return false;
        }
        ChunkFileInfoSendData metadata{};
        std::memcpy(&metadata, packet.payload, sizeof(metadata));
        auto& chunkInfo = static_cast<ChunkFileInfo&>(fileInfo);
        chunkInfo.FileId = metadata.FileId;
        chunkInfo.Size = metadata.Size;
        chunkInfo.ChunkSize = metadata.ChunkSize;
        chunkInfo.ChunkCount = metadata.ChunkCount;
        metadataSize = sizeof(metadata);
        nameLength = metadata.NameLength;
        pathLength = metadata.PathLength;
        break;
    }
    default:
        return false;
    }

    const auto stringsSize = static_cast<std::uint64_t>(nameLength) + pathLength;
    if (stringsSize > packet.payloadSize - metadataSize) {
        return false;
    }
    data = packet.payload + metadataSize;
    const std::string name(reinterpret_cast<const char*>(data), nameLength);
    const std::string path(reinterpret_cast<const char*>(data + nameLength), pathLength);
    try {
        fileInfo.Name = Utf8ToPath(name);
        fileInfo.Path = Utf8ToPath(path);
    } catch (const std::exception& e) {
        Report(std::string("[ERROR] ReceiverPacketToFileInfo: Failed to convert UTF-8 to path. Error: ") + e.what() + "\n");
        return false;
    }
    fileInfo.u8Name = fileInfo.Name.string();
    fileInfo.u8Path = fileInfo.Path.string();
    return true;
}

bool PacketReceiver::ReceiverPacketToChunk(const Packet& packet, ChunkFileHeader& chunk,
                                            const std::uint8_t*& data) const {
    if (packet.payloadSize < sizeof(ChunkFileHeader)) {
        return false;
    }
    std::memcpy(&chunk, packet.payload, sizeof(chunk));
    if (chunk.Size == 0 || packet.payloadSize != sizeof(chunk) + chunk.Size) {
        return false;
    }
    data = packet.payload + sizeof(chunk);
    return true;
}

bool PacketReceiver::PacketFileInfo(const Packet& packet) {
    ChunkFileInfo info;
    if (!ReceiverPacketToFileInfo(packet, info)) {
        Report("[ERROR] Received invalid file metadata");
        return false;
    }
    FileReceiver validation;
    validation.fileInfo = info;
    if (!validation.ValidFileInfo() || !IsSafeRelativePath(info.Path)) {
        Report("[ERROR] Received invalid file metadata");
        return false;
    }
    const fs::path destination = downloadDirectory_ / info.Path;
    const fs::path temporary = destination.string() + ".download";
    std::error_code error;
    fs::create_directories(destination.parent_path(), error);
    if (error) {
        Report("[ERROR] Failed to create destination directory: " + destination.parent_path().string());
        return false;
    }
    fs::remove(destination, error);
    if (error && error != std::errc::no_such_file_or_directory) {
        Report("[ERROR] Failed to replace destination file: " + destination.string());
        return false;
    }
    FileReceiver session;
    session.fileInfo = info;
    session.temporaryPath = temporary;
    session.received.resize(info.ChunkCount);
    session.file.open(session.temporaryPath, std::ios::binary | std::ios::trunc);
    if (!session.file.is_open()) {
        Report("[ERROR] Failed to open temporary file: " + session.temporaryPath.string());
        return false;
    }
    sessions_.insert_or_assign(info.FileId, std::move(session));
    return true;
}

bool PacketReceiver::PacketChunk(const Packet& packet) {
    ChunkFileHeader chunk{};
    const std::uint8_t* data = nullptr;
    if (!ReceiverPacketToChunk(packet, chunk, data)) {
        Report("[ERROR] Received invalid file chunk");
        return false;
    }
    const auto found = sessions_.find(chunk.FileId);
    if (found == sessions_.end() || !found->second.ReceiverChunk(chunk, data)) {
        Report("[ERROR] Failed to write file chunk");
        return false;
    }
    auto& session = found->second;
    if (session.receivedCount != session.fileInfo.ChunkCount) {
        return true;
    }
    session.file.close();
    const fs::path destination = downloadDirectory_ / session.fileInfo.Path;
    std::error_code error;
    fs::rename(session.temporaryPath, destination, error);
    if (error) {
        Report("[ERROR] Failed to finalize downloaded file: " + destination.string());
        return false;
    }
    sessions_.erase(found);
    return true;
}

void PacketReceiver::PacketDispatcher(const Packet& packet) {
    switch (packet.header.Type) {
    case PacketType::FILE_CHUNK:
        PacketChunk(packet);
        break;
    case PacketType::FILE_INFO:
        PacketFileInfo(packet);
        break;
    case PacketType::SEND_MESSAGE: {
        std::string message;
        if (ReceiverString(packet, message)) {
            connection_.NotifyMessage(message);
        }
        break;
    }
    default:
        Report("[ERROR] Received unsupported packet type");
        break;
    }
}

void PacketReceiver::Finish() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!buffer_.empty()) {
        Report("[ERROR] Stream was closed with an incomplete packet");
    }
    buffer_.clear();
}

} // namespace FileClient

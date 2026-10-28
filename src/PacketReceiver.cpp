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

std::error_code ReplaceDownloadedFile(const fs::path& temporary, const fs::path& destination) {
#ifdef _WIN32
    if (MoveFileExW(temporary.c_str(), destination.c_str(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        return {};
    }
    return {static_cast<int>(GetLastError()), std::system_category()};
#else
    std::error_code error;
    fs::rename(temporary, destination, error);
    return error;
#endif
}

} // namespace

bool FileReceiver::ReceiverChunk(const ChunkFileHeader& chunk, const std::uint8_t* data) {
    if (data == nullptr || !file.is_open() || chunk.Index >= fileInfo.ChunkCount ||
        chunk.Size == 0 || chunk.Size > fileInfo.ChunkSize) {
        return false;
    }
    const std::uint64_t offset = chunk.Index * fileInfo.ChunkSize;
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

StreamContext::StreamContext() = default;
StreamContext::~StreamContext() = default;

void StreamContext::EnableReceiver(Connection& owner) {
    if (!ReceiverManager) {
        ReceiverManager = std::make_unique<PacketReceiver>(owner);
    }
}

PacketReceiver::PacketReceiver(Connection& connection) : connection_(connection) {}

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

bool PacketReceiver::ReceiverUploadState(const Packet& packet, std::uint64_t& fileId,
                                        UploadState& state, TransferDirection& direction) const {
    if (packet.payload == nullptr || packet.payloadSize != sizeof(UploadStateSendData)) {
        Report("[ERROR] ReceiverUploadState: Invalid payload");
        return false;
    }
    UploadStateSendData data{};
    std::memcpy(&data, packet.payload, sizeof(data));
    // Значение пришло из сети, проверяем что оно входит в enum
    if (static_cast<std::uint8_t>(data.State) > static_cast<std::uint8_t>(UploadState::Failed)) {
        Report("[ERROR] ReceiverUploadState: Invalid state " + std::to_string(static_cast<int>(data.State)));
        return false;
    }
    if (static_cast<std::uint8_t>(data.Direction) > static_cast<std::uint8_t>(TransferDirection::ServerToClient)) {
        Report("[ERROR] ReceiverUploadState: Invalid direction " + std::to_string(static_cast<int>(data.Direction)));
        return false;
    }
    fileId = data.FileId;
    state = data.State;
    direction = data.Direction;
    return true;
}

bool PacketReceiver::PacketFileState(const Packet& packet) {
    std::uint64_t fileId = 0;
    UploadState state{};
    TransferDirection direction{};
    if (!ReceiverUploadState(packet, fileId, state, direction) || fileId == 0) {
        Report("[ERROR] PacketFileState: Invalid fileId");
        return false;
    }
    // ClientToServer: клиент отправляет файл (activeUploadTransfers_)
    // ServerToClient: клиент принимает файл (Connection::Sessions)
    const bool updated = direction == TransferDirection::ClientToServer
        ? connection_.SetStateUpload(fileId, state)
        : SetStateDownload(fileId, state);
    // Cancelled приходит в ответ на нашу же отмену, запись к этому моменту уже может быть удалена
    if (!updated && state != UploadState::Cancelled) {
        Report("[ERROR] PacketFileState: Failed to set state for fileId: " + std::to_string(fileId));
        return false;
    }
    return true;
}

// Состояние для файла, который клиент принимает от сервера (Connection::Sessions).
// Cancelled: удаляем временный файл и сессию. Failed: закрываем файл, но сохраняем
// временный файл и сессию с картой received (в будущем докачка потерянных чанков).
bool PacketReceiver::SetStateDownload(std::uint64_t fileId, UploadState state) {
    if (state == UploadState::Cancelled) {
        return connection_.CancelDownloadFile(fileId);
    }
    if (fileId == 0 || state != UploadState::Failed) {
        return false;
    }
    std::lock_guard<std::mutex> lock(connection_.sessionsMutex_);
    const auto found = connection_.Sessions.find(fileId);
    if (found == connection_.Sessions.end()) {
        return false;
    }
    found->second.file.close();
    return true;
}

void PacketReceiver::Receiver(const std::uint8_t* data, std::uint32_t size) {
    if (data == nullptr || size == 0) {
        return;
    }
    std::vector<ReceivedPacket> packets;
    bool packetTooLarge = false;
    {
        // Без мьютекса: buffer_ принадлежит одному потоку, MsQuic не вызывает его колбэк параллельно
        buffer_.insert(buffer_.end(), data, data + size);
        while (buffer_.size() >= sizeof(PacketHeader)) {
            PacketHeader header{};
            std::memcpy(&header, buffer_.data(), sizeof(header));
            if (header.Length > MAX_PACKET_SIZE) {
                buffer_.clear();
                packetTooLarge = true;
                break;
            }
            const auto totalSize = sizeof(PacketHeader) + static_cast<std::size_t>(header.Length);
            if (buffer_.size() < totalSize) {
                break;
            }
            ReceivedPacket packet;
            packet.header = header;
            packet.payload.assign(buffer_.begin() + sizeof(PacketHeader),
                                  buffer_.begin() + totalSize);
            packets.emplace_back(std::move(packet));
            buffer_.erase(buffer_.begin(), buffer_.begin() + totalSize);
        }
    }
    if (packetTooLarge) {
        Report("[ERROR] Received packet exceeds the protocol limit");
        return;
    }
    for (auto& packet : packets) {
        PacketDispatcher(std::move(packet));
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
    const fs::path destination = connection_.DownloadDirectory() / info.Path;
    const fs::path temporary = destination.string() + ".download";
    std::error_code error;
    fs::create_directories(destination.parent_path(), error);
    if (error) {
        Report("[ERROR] Failed to create destination directory: " + destination.parent_path().string());
        return false;
    }
    FileReceiver session;
    session.fileInfo = info;
    session.temporaryPath = temporary;
    session.received.resize(info.ChunkCount);
    bool duplicateSession = false;
    bool failedToOpen = false;
    {
        std::lock_guard<std::mutex> lock(connection_.sessionsMutex_);
        if (connection_.Sessions.find(info.FileId) != connection_.Sessions.end()) {
            duplicateSession = true;
        } else {
            session.file.open(session.temporaryPath, std::ios::binary | std::ios::trunc);
            if (!session.file.is_open()) {
                failedToOpen = true;
            } else {
                connection_.Sessions.emplace(info.FileId, std::move(session));
            }
        }
    }
    if (duplicateSession) {
        Report("[ERROR] Received duplicate metadata for active file transfer: " + std::to_string(info.FileId));
        return false;
    }
    if (failedToOpen) {
        Report("[ERROR] Failed to open temporary file: " + session.temporaryPath.string());
        return false;
    }
    return true;
}

bool PacketReceiver::PacketChunk(const Packet& packet) {
    ChunkFileHeader chunk{};
    const std::uint8_t* data = nullptr;
    if (!ReceiverPacketToChunk(packet, chunk, data)) {
        Report("[ERROR] Received invalid file chunk");
        return false;
    }
    std::string errorMessage;
    bool completed = false;
    // Берём до sessionsMutex_: DownloadDirectory() сам захватывает этот мьютекс
    const fs::path downloadDirectory = connection_.DownloadDirectory();
    {
        std::lock_guard<std::mutex> lock(connection_.sessionsMutex_);
        const auto found = connection_.Sessions.find(chunk.FileId);
        if (found == connection_.Sessions.end() || !found->second.ReceiverChunk(chunk, data)) {
            errorMessage = "[ERROR] Failed to write file chunk";
        } else {
            auto& session = found->second;
            if (session.receivedCount == session.fileInfo.ChunkCount) {
                session.file.close();
                const fs::path destination = downloadDirectory / session.fileInfo.Path;
                const std::error_code error =
                    ReplaceDownloadedFile(session.temporaryPath, destination);
                if (error) {
                    errorMessage = "[ERROR] Failed to finalize downloaded file: " + destination.string();
                } else {
                    connection_.Sessions.erase(found);
                    completed = true;
                }
            }
        }
    }
    if (!errorMessage.empty()) {
        Report(errorMessage);
        return false;
    }
    // Отправляем вне sessionsMutex_, чтобы не держать блокировку во время StreamSend
    if (completed) {
        const UploadStateSendData stateData{chunk.FileId, UploadState::Confirmed, TransferDirection::ServerToClient};
        if (!connection_.SendPacketImpl(connection_.commandStream(), PacketType::FILE_STATE,
                                        &stateData, static_cast<std::uint32_t>(sizeof(stateData)))) {
            Report("[ERROR] PacketChunk: Failed to send completion state for fileId: " + std::to_string(chunk.FileId));
        }
    }
    return true;
}

void PacketReceiver::PacketDispatcher(ReceivedPacket receivedPacket) {
    Packet packet{receivedPacket.header, receivedPacket.payload.data(),
                static_cast<std::uint32_t>(receivedPacket.payload.size())};
    if (!ValidPacket(packet)) {
        return;
    }
    switch (receivedPacket.header.Type) {
    case PacketType::FILE_INFO:
        PacketFileInfo(packet);
        break;
    case PacketType::FILE_CHUNK:
        PacketChunk(packet);
        break;
    case PacketType::SEND_MESSAGE: {
        std::string message;
        if (ReceiverString(packet, message)) {
            connection_.NotifyMessage(message);
        }
        break;
    }
    case PacketType::FILE_STATE:
        PacketFileState(packet);
        break;
    default:
        Report("[ERROR] Received unsupported packet type");
        break;
    }
}

void PacketReceiver::Finish() {
    const bool incompletePacket = !buffer_.empty();
    buffer_.clear();
    if (incompletePacket) {
        Report("[ERROR] Stream was closed with an incomplete packet");
    }
}

} // namespace FileClient

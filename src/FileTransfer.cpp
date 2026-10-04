#include <FileClient/FileTransfer.h>

#include <cstring>

#include <FileClient/Connection.h>
#include <FileClient/PacketSender.h>

namespace FileClient {

FileTransfer::FileTransfer(Connection& connection, FileReader& reader, PacketSender& sender)
    : connection_(connection), reader_(reader), sender_(sender) {}

bool FileTransfer::SendFileToServer(const fs::path& path, const fs::path& serverPath) {
    if (!connection_.IsConnected() || connection_.commandStream() == nullptr) {
        connection_.reporter().Report("[ERROR] Connection or command stream is not ready");
        return false;
    }

    std::vector<ChunkFileInfo> files;
    if (!reader_.ReadFiles(path, files, serverPath)) {
        connection_.reporter().Report("[ERROR] No non-empty files found: " + path.string());
        return false;
    }

    for (const auto& fileInfo : files) {
        const ChunkFileInfoSendData metadata{
            static_cast<std::uint32_t>(fileInfo.u8Name.size()),
            static_cast<std::uint32_t>(fileInfo.u8Path.size()),
            fileInfo.type,
            fileInfo.FileId,
            fileInfo.Size,
            fileInfo.ChunkSize,
            fileInfo.ChunkCount};
        const auto payloadSize = sizeof(metadata) + fileInfo.u8Name.size() + fileInfo.u8Path.size();
        if (payloadSize > MAX_PACKET_SIZE) {
            connection_.reporter().Report("[ERROR] File metadata is too large: " + fileInfo.ClientPath.string());
            return false;
        }
        std::vector<std::uint8_t> payload(payloadSize);
        std::memcpy(payload.data(), &metadata, sizeof(metadata));
        std::memcpy(payload.data() + sizeof(metadata), fileInfo.u8Name.data(), fileInfo.u8Name.size());
        std::memcpy(payload.data() + sizeof(metadata) + fileInfo.u8Name.size(),
                    fileInfo.u8Path.data(), fileInfo.u8Path.size());
        if (!sender_.SendPacket(connection_.commandStream(), PacketType::FILE_INFO, payload.data(),
                                static_cast<std::uint32_t>(payload.size()))) {
            connection_.reporter().Report("[ERROR] Failed to send file metadata: " + fileInfo.ClientPath.string());
            return false;
        }

        const std::uint32_t streamCount = fileInfo.Size < 512ULL * 1024ULL * 1024ULL ? 1U :
            fileInfo.Size < 1024ULL * 1024ULL * 1024ULL ? 2U :
            fileInfo.Size < 2ULL * 1024ULL * 1024ULL * 1024ULL ? 3U : 4U;
        for (std::uint32_t index = 0; index < streamCount; ++index) {
            auto* context = new UploadStreamContext();
            context->Owner = &connection_;
            context->FileInfo = fileInfo;
            context->ChunkIndex = index;
            context->ChunkStep = streamCount;
            context->File.open(fileInfo.ClientPath, std::ios::binary);
            if (!context->File.is_open()) {
                connection_.reporter().Report("[ERROR] Failed to open file: " + fileInfo.ClientPath.string());
                delete context;
                return false;
            }
            HQUIC stream = nullptr;
            if (!connection_.CreateStream(StreamType::UPLOAD, stream, context)) {
                context->File.close();
                delete context;
                return false;
            }
            connection_.UploadStreams.emplace(stream, context);
        }
    }
    return true;
}

bool FileTransfer::ManagementStorage(PacketType type, const fs::path& path,
                                    const fs::path& name, bool replacement) {
    if (!connection_.IsConnected() || connection_.commandStream() == nullptr || path.empty() || name.empty()) {
        connection_.reporter().Report("[ERROR] Connection, path, or name is invalid");
        return false;
    }

    FileInfo info;
    info.Path = path;
    switch (type) {
    case PacketType::MOVE_FILE:
        info.Name = name / path.filename();
        break;
    case PacketType::RENAME:
        info.Name = path.parent_path() / name;
        break;
    default:
        info.Name = path / name;
        break;
    }
    info.u8Name = info.Name.u8string();
    info.u8Path = info.Path.u8string();
    const ManagerFileInfoSendData metadata{
        {static_cast<std::uint32_t>(info.u8Name.size()), static_cast<std::uint32_t>(info.u8Path.size()),
        FileInfoType::Manager},
        replacement};
    const auto payloadSize = sizeof(metadata) + info.u8Name.size() + info.u8Path.size();
    if (payloadSize > MAX_PACKET_SIZE) {
        connection_.reporter().Report("[ERROR] Storage-management metadata is too large");
        return false;
    }
    std::vector<std::uint8_t> payload(payloadSize);
    std::memcpy(payload.data(), &metadata, sizeof(metadata));
    std::memcpy(payload.data() + sizeof(metadata), info.u8Name.data(), info.u8Name.size());
    std::memcpy(payload.data() + sizeof(metadata) + info.u8Name.size(), info.u8Path.data(), info.u8Path.size());
    return sender_.SendPacket(connection_.commandStream(), type, payload.data(),
                            static_cast<std::uint32_t>(payload.size()));
}

bool FileTransfer::InitiateFileTransfer(const fs::path& path) {
    if (!connection_.IsConnected() || connection_.commandStream() == nullptr || path.empty()) {
        connection_.reporter().Report("[ERROR] Connection or requested path is invalid");
        return false;
    }
    const std::string value = path.u8string();
    if (value.size() > MAX_PACKET_SIZE) {
        connection_.reporter().Report("[ERROR] Requested path is too long");
        return false;
    }
    return sender_.SendPacket(connection_.commandStream(), PacketType::COMMAND_REQUEST_FILE,
                            value.data(), static_cast<std::uint32_t>(value.size()));
}

} // namespace FileClient

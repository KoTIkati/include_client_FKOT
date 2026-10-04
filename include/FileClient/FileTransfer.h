#pragma once

#include <filesystem>

#include <FileClient/FileReader.h>

namespace FileClient {

class Connection;
class PacketSender;

class FileTransfer {
public:
    FileTransfer(Connection& connection, FileReader& reader, PacketSender& sender);

    bool SendFileToServer(const fs::path& path, const fs::path& serverPath);
    bool ManagementStorage(PacketType type, const fs::path& path,
                            const fs::path& name, bool replacement = false);
    bool InitiateFileTransfer(const char* path);

private:
    Connection& connection_;
    FileReader& reader_;
    PacketSender& sender_;
};

} // namespace FileClient

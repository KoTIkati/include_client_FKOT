#pragma once

#include <filesystem>
#include <functional>
#include <memory>
#include <string>

#include <FileClient/Connection.h>
#include <FileClient/Error.h>
#include <FileClient/FileTransfer.h>
#include <FileClient/PacketReceiver.h>
#include <FileClient/PacketSender.h>

namespace FileClient {

class Client {
public:
    using MessageHandler = Connection::MessageHandler;

    Client();
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    bool connect(const char* host, std::uint16_t port);
    bool Connect(const char* host, std::uint16_t port) { return connect(host, port); }
    bool waitForConnection(std::chrono::milliseconds timeout = std::chrono::seconds(10));
    bool WaitForConnection(std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
        return waitForConnection(timeout);
    }
    void disconnect();
    void Disconnect() { disconnect(); }
    bool isConnected() const noexcept;
    bool IsConnected() const noexcept { return isConnected(); }

    bool sendFile(const std::filesystem::path& path,
                const std::filesystem::path& serverPath = ".");
    bool SendFileToServer(const std::filesystem::path& path,
                            const std::filesystem::path& serverPath = ".") {
        return sendFile(path, serverPath);
    }
    bool managementStorage(PacketType type, const std::filesystem::path& path,
                            const std::filesystem::path& name, bool replacement = false);
    bool sendMessage(const std::string& text);

    bool SendPacket(HQUIC stream, PacketType type, const void* payload,
                    std::uint32_t payloadSize, std::uint32_t requestId = 0);
    bool sendPacket(const void* data, std::size_t size);

    bool stopDownloadFile(std::uint64_t fileId);
    bool stopUploadFile(std::uint64_t fileId);

    Connection& getConnection() noexcept { return connection_; }
    const Connection& getConnection() const noexcept { return connection_; }
    void setMessageHandler(MessageHandler handler);
    void setErrorCallback(ErrorCallback callback);
    void setDownloadDirectory(const std::filesystem::path& directory);

private:
    ErrorReporter reporter_;
    MsQuic msQuic_;
    Connection connection_;
    PacketSender sender_;
    FileReader reader_;
    FileTransfer transfer_;
};

} // namespace FileClient
#include <chrono>

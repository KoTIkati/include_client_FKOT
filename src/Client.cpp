#include <FileClient/Client.h>

namespace FileClient {

Client::Client()
    : msQuic_(reporter_),
    connection_(msQuic_, reporter_),
    sender_(connection_),
    transfer_(connection_, reader_, sender_) {
    connection_.SetUploadHandler([this](HQUIC stream, UploadStreamContext& context) {
        return reader_.QueueNextChunk(stream, context, sender_);
    });
}

Client::~Client() {
    connection_.SetUploadHandler({});
    disconnect();
}

bool Client::connect(const char* host, std::uint16_t port) {
    return connection_.Connect(host, port) && waitForConnection(std::chrono::seconds(10));
}

bool Client::waitForConnection(std::chrono::milliseconds timeout) {
    return connection_.WaitForConnection(timeout);
}

void Client::disconnect() {
    connection_.Disconnect();
}

bool Client::isConnected() const noexcept {
    return connection_.IsConnected();
}

bool Client::sendFile(const std::filesystem::path& path, const std::filesystem::path& serverPath) {
    return transfer_.SendFileToServer(path, serverPath);
}

bool Client::managementStorage(PacketType type, const std::filesystem::path& path,
                                const std::filesystem::path& name, bool replacement) {
    return transfer_.ManagementStorage(type, path, name, replacement);
}

bool Client::stopDownloadFile(std::uint64_t fileId){
    return transfer_.StopDownloadFile(fileId);
}

bool Client::stopUploadFile(std::uint64_t fileId){
    return transfer_.StopUploadFile(fileId);
}

bool Client::sendMessage(const std::string& text) {
    return sender_.SendPacket(connection_.commandStream(), PacketType::SEND_MESSAGE,
                                text.data(), static_cast<std::uint32_t>(text.size()));
}

bool Client::SendPacket(HQUIC stream, PacketType type, const void* payload,
                        std::uint32_t payloadSize, std::uint32_t requestId) {
    return sender_.SendPacket(stream, type, payload, payloadSize, requestId);
}

bool Client::sendPacket(const void* data, std::size_t size) {
    if (size > MAX_PACKET_SIZE) {
        reporter_.Report("[ERROR] Packet exceeds the protocol limit");
        return false;
    }
    return sender_.SendPacket(connection_.commandStream(), PacketType::SEND_MESSAGE, data,
                                static_cast<std::uint32_t>(size));
}

void Client::setMessageHandler(MessageHandler handler) {
    connection_.SetMessageHandler(std::move(handler));
}

void Client::setErrorCallback(ErrorCallback callback) {
    reporter_.SetCallback(std::move(callback));
}

void Client::setDownloadDirectory(const std::filesystem::path& directory) {
    connection_.SetDownloadDirectory(directory);
}

} // namespace FileClient

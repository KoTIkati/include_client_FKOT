#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <Windows.h>
#endif

#include <msquic.h>

#include <FileClient/Chunk.h>
#include <FileClient/Error.h>
#include <FileClient/MsQuic.h>
#include <FileClient/Protocol.h>

namespace FileClient {

class PacketReceiver;

class Connection {
public:
    using MessageHandler = std::function<void(const std::string&)>;
    using UploadHandler = std::function<bool(HQUIC stream, UploadStreamContext& context)>;

    Connection(MsQuic& msQuic, ErrorReporter& reporter);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    bool Connect(const char* host, std::uint16_t port);
    bool WaitForConnection(std::chrono::milliseconds timeout);
    void Disconnect();
    bool IsConnected() const noexcept { return connected_.load(std::memory_order_acquire); }

    HQUIC commandStream() const noexcept { return commandStream_; }
    HQUIC CommandStream() const noexcept { return commandStream(); }

    bool CreateStream(StreamType type, HQUIC& stream, StreamContext* context);
    bool StreamOpen(HQUIC& stream, StreamContext* context) {
        return CreateStream(context == nullptr ? StreamType::COMMAND : context->type, stream, context);
    }
    bool SendPacketImpl(HQUIC stream, PacketType type, const void* payload,
                        std::uint32_t payloadSize, std::uint32_t requestId = 0);
    bool SendChunkImpl(HQUIC stream, const ChunkFileHeader& chunk,
                        const std::vector<std::uint8_t>& data);

    void SetMessageHandler(MessageHandler handler);
    void SetDownloadDirectory(const fs::path& directory);
    fs::path DownloadDirectory() const;
    void SetUploadHandler(UploadHandler handler);
    std::shared_ptr<UploadTransferState> BeginUpload(std::uint64_t fileId);
    void FinishUploadRegistration(std::uint64_t fileId,
                                  const std::shared_ptr<UploadTransferState>& transfer);
    bool CancelUploadStreams(std::uint64_t fileId);
    bool SetStateUpload(std::uint64_t fileId, UploadState state);
    bool CancelDownloadFile(std::uint64_t fileId);
    void NotifyMessage(const std::string& message) const;
    ErrorReporter& reporter() noexcept { return reporter_; }
    const QUIC_API_TABLE* api() const noexcept { return msQuic_.api(); }

    std::unordered_map<HQUIC, StreamContext*> UploadStreams;
    std::unordered_map<HQUIC, StreamContext*> DownloadStreams;

    // Сессии приёма файлов от сервера, общие для всех потоков соединения
    std::unordered_map<std::uint64_t, FileReceiver> Sessions;
    mutable std::mutex sessionsMutex_; // защищает Sessions и downloadDirectory_

private:
    struct SendContext {
        std::vector<std::uint8_t> buffer;
        QUIC_BUFFER quicBuffer{};
    };

    static QUIC_STATUS QUIC_API ConnectionCallback(HQUIC connection, void* context,
                                                    QUIC_CONNECTION_EVENT* event);
    static QUIC_STATUS QUIC_API StreamCallback(HQUIC stream, void* context,
                                                QUIC_STREAM_EVENT* event);
    QUIC_STATUS HandleConnectionEvent(QUIC_CONNECTION_EVENT* event);
    QUIC_STATUS HandleStreamEvent(HQUIC stream, StreamContext* context,
                                    QUIC_STREAM_EVENT* event);
    void CloseStream(HQUIC stream, StreamContext* context) noexcept;
    void ReportStatus(const std::string& operation, QUIC_STATUS status) const;
    void SetConnectionState(bool connected);

    enum class ConnectionState {
        Disconnected,
        Connecting,
        Connected,
        Failed
    };

    MsQuic& msQuic_;
    ErrorReporter& reporter_;
    HQUIC connection_ = nullptr;
    HQUIC configuration_ = nullptr;
    HQUIC commandStream_ = nullptr;
    std::atomic_bool connected_{false};
    std::atomic_uint32_t nextRequestId_{1};
    mutable std::mutex connectionStateMutex_;
    std::condition_variable connectionStateChanged_;
    ConnectionState connectionState_ = ConnectionState::Disconnected;
    mutable std::mutex mutex_;
    mutable std::mutex streamsMutex_;
    std::unordered_map<std::uint64_t, std::shared_ptr<UploadTransferState>> activeUploadTransfers_;
    MessageHandler messageHandler_;
    UploadHandler uploadHandler_;
    fs::path downloadDirectory_ = ".";
};

using ClientConnection = Connection;

} // namespace FileClient

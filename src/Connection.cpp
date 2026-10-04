#include <FileClient/Connection.h>

#include <cstring>
#include <iomanip>
#include <sstream>

#include <FileClient/PacketReceiver.h>

namespace FileClient {

Connection::Connection(MsQuic& msQuic, ErrorReporter& reporter)
    : msQuic_(msQuic), reporter_(reporter) {}

Connection::~Connection() {
    Disconnect();
}

void Connection::ReportStatus(const std::string& operation, QUIC_STATUS status) const {
    std::ostringstream stream;
    stream << "[ERROR] " << operation << " failed: 0x" << std::hex
           << static_cast<std::uint32_t>(status);
    reporter_.Report(stream.str());
}

bool Connection::Connect(const char* host, std::uint16_t port) {
    if (std::strlen(host) == 0 || port == 0) {
        reporter_.Report("[ERROR] Host or port is invalid");
        return false;
    }
    if (connection_ != nullptr) {
        reporter_.Report("[ERROR] A connection is already open");
        return false;
    }
    if (!msQuic_.Initialize()) {
        return false;
    }

    const QUIC_BUFFER alpn = { (uint32_t)strlen("kot-storage-v1"), (uint8_t*)"kot-storage-v1" };

    QUIC_SETTINGS settings = {};
    settings.IsSet.IdleTimeoutMs = TRUE;
    settings.IdleTimeoutMs = 300000;
    settings.IsSet.PeerBidiStreamCount = TRUE;
    settings.PeerBidiStreamCount = 100;
    settings.IsSet.TlsClientMaxSendBuffer = TRUE;
    settings.TlsClientMaxSendBuffer = 0x800000;
    settings.IsSet.KeepAliveIntervalMs = TRUE;
    settings.KeepAliveIntervalMs = 5000;
    settings.IsSet.DisconnectTimeoutMs = TRUE;
    settings.DisconnectTimeoutMs = 10000;

    QUIC_STATUS status = msQuic_.api()->ConfigurationOpen(
        msQuic_.registration(), &alpn, 1, &settings, sizeof(settings), this, &configuration_);
    if (QUIC_FAILED(status)) {
        ReportStatus("ConfigurationOpen", status);
        return false;
    }
    QUIC_CREDENTIAL_CONFIG credentials{};
    credentials.Type = QUIC_CREDENTIAL_TYPE_NONE;
    credentials.Flags = QUIC_CREDENTIAL_FLAG_CLIENT | QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
    status = msQuic_.api()->ConfigurationLoadCredential(configuration_, &credentials);
    if (QUIC_FAILED(status)) {
        ReportStatus("ConfigurationLoadCredential", status);
        msQuic_.api()->ConfigurationClose(configuration_);
        configuration_ = nullptr;
        return false;
    }
    status = msQuic_.api()->ConnectionOpen(msQuic_.registration(), ConnectionCallback, this, &connection_);
    if (QUIC_FAILED(status)) {
        ReportStatus("ConnectionOpen", status);
        msQuic_.api()->ConfigurationClose(configuration_);
        configuration_ = nullptr;
        return false;
    }
    status = msQuic_.api()->ConnectionStart(connection_, configuration_, QUIC_ADDRESS_FAMILY_INET,
                                            host, port);
    if (QUIC_FAILED(status)) {
        ReportStatus("ConnectionStart", status);
        msQuic_.api()->ConnectionClose(connection_);
        connection_ = nullptr;
        msQuic_.api()->ConfigurationClose(configuration_);
        configuration_ = nullptr;
        return false;
    }
    return true;
}

void Connection::Disconnect() {
    const QUIC_API_TABLE* api = msQuic_.api();
    if (api == nullptr) {
        return;
    }
    for (auto& entry : UploadStreams) {
        if (entry.first != nullptr) {
            api->StreamShutdown(entry.first, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            api->StreamClose(entry.first);
        }
        delete entry.second;
    }
    UploadStreams.clear();
    for (auto& entry : DownloadStreams) {
        if (entry.first != nullptr) {
            api->StreamShutdown(entry.first, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            api->StreamClose(entry.first);
        }
        delete entry.second;
    }
    DownloadStreams.clear();
    if (commandStream_ != nullptr) {
        api->StreamShutdown(commandStream_, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
        api->StreamClose(commandStream_);
        commandStream_ = nullptr;
    }
    if (connection_ != nullptr) {
        api->ConnectionShutdown(connection_, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
        api->ConnectionClose(connection_);
        connection_ = nullptr;
    }
    if (configuration_ != nullptr) {
        api->ConfigurationClose(configuration_);
        configuration_ = nullptr;
    }
    connected_.store(false, std::memory_order_release);
}

bool Connection::CreateStream(StreamType type, HQUIC& stream, StreamContext* context) {
    stream = nullptr;
    if (connection_ == nullptr || context == nullptr || msQuic_.api() == nullptr) {
        reporter_.Report("[ERROR] Cannot create stream without an active connection");
        return false;
    }
    context->Owner = this;
    const auto flags = type == StreamType::DOWNLOAD ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL
                                                     : QUIC_STREAM_OPEN_FLAG_NONE;
    QUIC_STATUS status = msQuic_.api()->StreamOpen(connection_, flags, StreamCallback, context, &stream);
    if (QUIC_FAILED(status)) {
        ReportStatus("StreamOpen", status);
        stream = nullptr;
        return false;
    }
    status = msQuic_.api()->StreamStart(stream, QUIC_STREAM_START_FLAG_NONE);
    if (QUIC_FAILED(status)) {
        ReportStatus("StreamStart", status);
        msQuic_.api()->StreamClose(stream);
        stream = nullptr;
        return false;
    }
    if (type == StreamType::COMMAND) {
        commandStream_ = stream;
    }
    return true;
}

bool Connection::SendPacketImpl(HQUIC stream, PacketType type, const void* payload,
                                std::uint32_t payloadSize, std::uint32_t requestId) {
    if (stream == nullptr || (payloadSize != 0 && payload == nullptr) || payloadSize > MAX_PACKET_SIZE ||
        msQuic_.api() == nullptr) {
        reporter_.Report("[ERROR] Invalid packet arguments");
        return false;
    }
    auto* context = new SendContext();
    try {
        context->buffer.resize(sizeof(PacketHeader) + payloadSize);
    } catch (const std::exception&) {
        delete context;
        reporter_.Report("[ERROR] Failed to allocate a packet buffer");
        return false;
    }
    const PacketHeader header{PROTOCOL_VERSION, type, payloadSize,
                                requestId == 0 ? nextRequestId_.fetch_add(1, std::memory_order_relaxed) : requestId};
    std::memcpy(context->buffer.data(), &header, sizeof(header));
    if (payloadSize != 0) {
        std::memcpy(context->buffer.data() + sizeof(header), payload, payloadSize);
    }
    context->quicBuffer.Buffer = context->buffer.data();
    context->quicBuffer.Length = static_cast<std::uint32_t>(context->buffer.size());
    const QUIC_STATUS status = msQuic_.api()->StreamSend(stream, &context->quicBuffer, 1,
                                                        QUIC_SEND_FLAG_NONE, context);
    if (QUIC_FAILED(status)) {
        ReportStatus("StreamSend", status);
        delete context;
        return false;
    }
    return true;
}

bool Connection::SendChunkImpl(HQUIC stream, const ChunkFileHeader& chunk,
                                const std::vector<std::uint8_t>& data) {
    if (data.size() != chunk.Size || data.size() + sizeof(chunk) > MAX_PACKET_SIZE) {
        reporter_.Report("[ERROR] Invalid chunk arguments");
        return false;
    }
    std::vector<std::uint8_t> payload(sizeof(chunk) + data.size());
    std::memcpy(payload.data(), &chunk, sizeof(chunk));
    if (!data.empty()) {
        std::memcpy(payload.data() + sizeof(chunk), data.data(), data.size());
    }
    return SendPacketImpl(stream, PacketType::FILE_CHUNK, payload.data(),
                            static_cast<std::uint32_t>(payload.size()));
}

void Connection::SetMessageHandler(MessageHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    messageHandler_ = std::move(handler);
}

void Connection::SetDownloadDirectory(const fs::path& directory) {
    if (receiver_ != nullptr) {
        receiver_->SetDownloadDirectory(directory);
    }
}

void Connection::SetPacketReceiver(PacketReceiver* receiver) noexcept {
    receiver_ = receiver;
}

void Connection::SetUploadHandler(UploadHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    uploadHandler_ = std::move(handler);
}

void Connection::NotifyMessage(const std::string& message) const {
    MessageHandler handler;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        handler = messageHandler_;
    }
    if (handler) {
        handler(message);
    }
}

QUIC_STATUS QUIC_API Connection::ConnectionCallback(HQUIC, void* context,
                                                    QUIC_CONNECTION_EVENT* event) {
    auto* connection = static_cast<Connection*>(context);
    if (connection == nullptr || event == nullptr) {
        return QUIC_STATUS_INVALID_PARAMETER;
    }
    try {
        return connection->HandleConnectionEvent(event);
    } catch (const std::exception& exception) {
        connection->reporter_.Report(std::string("[ERROR] Connection callback exception: ") + exception.what());
        return QUIC_STATUS_INTERNAL_ERROR;
    }
}

QUIC_STATUS Connection::HandleConnectionEvent(QUIC_CONNECTION_EVENT* event) {
    switch (event->Type) {
    case QUIC_CONNECTION_EVENT_CONNECTED: {
        connected_.store(true, std::memory_order_release);
        if (commandStream_ == nullptr) {
            auto* context = new CommandStreamContext();
            if (!CreateStream(StreamType::COMMAND, commandStream_, context)) {
                delete context;
                return QUIC_STATUS_INTERNAL_ERROR;
            }
        }
        break;
    }
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
        const HQUIC stream = event->PEER_STREAM_STARTED.Stream;
        if (commandStream_ == nullptr) {
            auto* context = new CommandStreamContext();
            context->Owner = this;
            commandStream_ = stream;
            msQuic_.api()->SetCallbackHandler(stream, reinterpret_cast<void*>(StreamCallback), context);
        } else {
            auto* context = new DownloadStreamContext();
            context->Owner = this;
            DownloadStreams.emplace(stream, context);
            msQuic_.api()->SetCallbackHandler(stream, reinterpret_cast<void*>(StreamCallback), context);
        }
        break;
    }
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        ReportStatus("Connection transport shutdown", event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        reporter_.Report("[ERROR] Connection shutdown was initiated by peer");
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        connected_.store(false, std::memory_order_release);
        commandStream_ = nullptr;
        connection_ = nullptr;
        break;
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

QUIC_STATUS QUIC_API Connection::StreamCallback(HQUIC stream, void* context,
                                                QUIC_STREAM_EVENT* event) {
    auto* streamContext = static_cast<StreamContext*>(context);
    if (streamContext == nullptr || streamContext->Owner == nullptr || event == nullptr) {
        return QUIC_STATUS_INVALID_PARAMETER;
    }
    try {
        return streamContext->Owner->HandleStreamEvent(stream, streamContext, event);
    } catch (const std::exception& exception) {
        streamContext->Owner->reporter_.Report(std::string("[ERROR] Stream callback exception: ") + exception.what());
        return QUIC_STATUS_INTERNAL_ERROR;
    }
}

QUIC_STATUS Connection::HandleStreamEvent(HQUIC stream, StreamContext* context,
                                        QUIC_STREAM_EVENT* event) {
    switch (event->Type) {
    case QUIC_STREAM_EVENT_START_COMPLETE:
        if (context->type == StreamType::UPLOAD) {
            UploadHandler handler;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                handler = uploadHandler_;
            }
            if (!handler || !handler(stream, *static_cast<UploadStreamContext*>(context))) {
                reporter_.Report("[ERROR] Failed to queue first upload chunk");
                msQuic_.api()->StreamShutdown(stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            }
        }
        break;
    case QUIC_STREAM_EVENT_RECEIVE:
        if (receiver_ != nullptr) {
            for (std::uint32_t index = 0; index < event->RECEIVE.BufferCount; ++index) {
                const auto& buffer = event->RECEIVE.Buffers[index];
                receiver_->Receiver(buffer.Buffer, buffer.Length);
            }
        }
        // msQuic_.api()->StreamReceiveComplete(stream, event->RECEIVE.TotalBufferLength);
        break;
    case QUIC_STREAM_EVENT_SEND_COMPLETE:
        delete static_cast<SendContext*>(event->SEND_COMPLETE.ClientContext);
        if (context->type == StreamType::UPLOAD) {
            auto& upload = *static_cast<UploadStreamContext*>(context);
            if (upload.PendingSendChunk > 0) {
                --upload.PendingSendChunk;
            }
            UploadHandler handler;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                handler = uploadHandler_;
            }
            if (!upload.Finished && (!handler || !handler(stream, upload))) {
                reporter_.Report("[ERROR] Failed to queue next upload chunk");
                msQuic_.api()->StreamShutdown(stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            }
        }
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        if (receiver_ != nullptr) {
            receiver_->Finish();
        }
        break;
    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        CloseStream(stream, context);
        break;
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

void Connection::CloseStream(HQUIC stream, StreamContext* context) noexcept {
    if (context->type == StreamType::UPLOAD) {
        UploadStreams.erase(stream);
    } else if (context->type == StreamType::DOWNLOAD) {
        DownloadStreams.erase(stream);
    } else if (commandStream_ == stream) {
        commandStream_ = nullptr;
    }
    delete context;
    if (stream != nullptr && msQuic_.api() != nullptr) {
        msQuic_.api()->StreamClose(stream);
    }
}

} // namespace FileClient

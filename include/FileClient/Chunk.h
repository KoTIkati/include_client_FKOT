#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <vector>

#include <msquic.h>

#include <FileClient/FileInfo.h>

namespace FileClient {

class Connection;
class PacketReceiver;

// Сессия приёма файла от сервера. Живёт в Connection::Sessions, так как FILE_INFO
// приходит по command stream, а чанки по другим потокам.
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

struct StreamContext {
    Connection* Owner = nullptr;
    StreamType type = StreamType::COMMAND;
    // Свой приёмник на каждый поток: байты разных потоков не смешиваются в одном буфере
    std::unique_ptr<PacketReceiver> ReceiverManager;

    // Определены в PacketReceiver.cpp, так как здесь PacketReceiver неполный тип
    StreamContext();
    virtual ~StreamContext();
    void EnableReceiver(Connection& owner);
};

struct CommandStreamContext final : StreamContext {
    CommandStreamContext() { type = StreamType::COMMAND; }
};

struct UploadTransferState {
    std::atomic_bool cancelled{false};
    std::size_t activeStreams = 0;
    bool registrationComplete = false;
    UploadState state = UploadState::Sending; // под streamsMutex_

    // Confirmed и Cancelled удаляют запись. Failed запись сохраняет (в будущем докачка потерянных чанков)
    bool CanErase() const {
        return state == UploadState::Confirmed || state == UploadState::Cancelled;
    }
};

struct UploadStreamContext final : StreamContext {
    UploadStreamContext() { type = StreamType::UPLOAD; }
    ChunkFileInfo FileInfo;
    std::ifstream File;
    std::uint64_t ChunkIndex = 0;
    std::uint64_t ChunkStep = 1;
    std::uint32_t PendingSendChunk = 0;
    bool Finished = false;
    std::shared_ptr<UploadTransferState> Transfer;
};

struct DownloadStreamContext final : StreamContext {
    DownloadStreamContext() { type = StreamType::DOWNLOAD; }
};

} // namespace FileClient

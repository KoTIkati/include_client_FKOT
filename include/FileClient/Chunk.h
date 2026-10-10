#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>

#include <msquic.h>

#include <FileClient/FileInfo.h>

namespace FileClient {

class Connection;

struct StreamContext {
    Connection* Owner = nullptr;
    StreamType type = StreamType::COMMAND;
    virtual ~StreamContext() = default;
};

struct CommandStreamContext final : StreamContext {
    CommandStreamContext() { type = StreamType::COMMAND; }
};

struct UploadTransferState {
    std::atomic_bool cancelled{false};
    std::size_t activeStreams = 0;
    bool registrationComplete = false;
};

struct UploadStreamContext final : StreamContext {
    UploadStreamContext() { type = StreamType::UPLOAD; }
    ChunkFileInfo FileInfo;
    std::ifstream File;
    std::uint32_t ChunkIndex = 0;
    std::uint32_t ChunkStep = 1;
    std::uint32_t PendingSendChunk = 0;
    bool Finished = false;
    std::shared_ptr<UploadTransferState> Transfer;
};

struct DownloadStreamContext final : StreamContext {
    DownloadStreamContext() { type = StreamType::DOWNLOAD; }
};

} // namespace FileClient

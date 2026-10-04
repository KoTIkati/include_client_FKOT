#pragma once

#include <cstdint>
#include <fstream>

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

struct UploadStreamContext final : StreamContext {
    UploadStreamContext() { type = StreamType::UPLOAD; }
    ChunkFileInfo FileInfo;
    std::ifstream File;
    std::uint32_t ChunkIndex = 0;
    std::uint32_t ChunkStep = 1;
    std::uint32_t PendingSendChunk = 0;
    bool Finished = false;
};

struct DownloadStreamContext final : StreamContext {
    DownloadStreamContext() { type = StreamType::DOWNLOAD; }
};

} // namespace FileClient

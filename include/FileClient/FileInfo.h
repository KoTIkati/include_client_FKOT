#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include <FileClient/Protocol.h>

namespace FileClient {

namespace fs = std::filesystem;

struct FileInfo {
    fs::path Name;
    fs::path Path;
    std::string u8Name;
    std::string u8Path;
    FileInfoType type;

    explicit FileInfo(FileInfoType value = FileInfoType::Default) : type(value) {}
    virtual ~FileInfo() = default;
};

struct ManagerFileInfo : FileInfo {
    ManagerFileInfo() : FileInfo(FileInfoType::Manager) {}
    bool Replacement = false;
};

struct ChunkFileInfo : FileInfo {
    ChunkFileInfo() : FileInfo(FileInfoType::Chunk) {}
    std::uint64_t FileId = 0;
    std::uint64_t Size = 0;
    std::uint32_t ChunkSize = 0;
    std::uint32_t ChunkCount = 0;
    fs::path ClientPath;
};

#pragma pack(push, 1)
struct FileInfoSendData {
    std::uint32_t NameLength;
    std::uint32_t PathLength;
    FileInfoType type;
};

struct ManagerFileInfoSendData : FileInfoSendData { bool Replacement; };

struct ChunkFileInfoSendData : FileInfoSendData {
    std::uint64_t FileId;
    std::uint64_t Size;
    std::uint32_t ChunkSize;
    std::uint32_t ChunkCount;
};

struct ChunkFileHeader {
    std::uint64_t FileId;
    std::uint32_t Index;
    std::uint32_t Size;
};
#pragma pack(pop)

} // namespace FileClient

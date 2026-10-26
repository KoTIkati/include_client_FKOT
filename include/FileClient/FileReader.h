#pragma once

#include <atomic>
#include <vector>

#include <FileClient/Chunk.h>

namespace FileClient {

class PacketSender;

class FileReader {
public:
    ChunkFileInfo ReadFileInfo(const fs::path& path, const fs::path& serverPath,
                                const fs::path& basePath);
    bool ReadFiles(const fs::path& path, std::vector<ChunkFileInfo>& files,
                    const fs::path& serverPath);
    bool ReadNextChunk(UploadStreamContext& context, ChunkFileHeader& chunk,
                    std::vector<std::uint8_t>& data) const;
    bool ReadNextFileChunk(const ChunkFileInfo& fileInfo, ChunkFileHeader& chunk,
                        std::ifstream& file, std::uint64_t chunkIndex,
                        std::vector<std::uint8_t>& data) const;
    bool QueueNextChunk(HQUIC stream, UploadStreamContext& context,
                        PacketSender& sender) const;
    bool QueueNextChunk(HQUIC stream, UploadStreamContext* context,
                        PacketSender& sender) const;
    std::vector<ChunkFileInfo> ReadFileAndDirectory(const fs::path& path,
                                                    std::vector<ChunkFileInfo>& files,
                                                    const fs::path& serverPath);

private:
    static fs::path CleanPath(const fs::path& fullPath, const fs::path& targetPath);
    std::atomic_uint64_t nextFileId_{1};
};

} // namespace FileClient

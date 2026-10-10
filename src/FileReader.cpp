#include <FileClient/FileReader.h>

#include <FileClient/PacketSender.h>

namespace FileClient {

ChunkFileInfo FileReader::ReadFileInfo(const fs::path& path, const fs::path& serverPath,
                                        const fs::path& basePath) {
    ChunkFileInfo info;
    std::error_code error;
    if (!fs::is_regular_file(path, error) || error) {
        return info;
    }

    info.FileId = nextFileId_.fetch_add(1, std::memory_order_relaxed);
    info.Name = path.filename();
    info.u8Name = info.Name.u8string();
    info.Path = serverPath / basePath;
    info.u8Path = info.Path.u8string();
    info.Size = fs::file_size(path, error);
    if (error || info.Size == 0) {
        return ChunkFileInfo{};
    }
    info.ClientPath = path;
    info.ChunkSize = info.Size < 1024ULL * 1024ULL * 1024ULL ? 1024U * 1024U : 8U * 1024U * 1024U;
    info.ChunkCount = static_cast<std::uint32_t>((info.Size + info.ChunkSize - 1) / info.ChunkSize);
    return info;
}

fs::path FileReader::CleanPath(const fs::path& fullPath, const fs::path& targetPath) {
    fs::path result;
    auto current = fullPath.begin();
    for (; current != fullPath.end(); ++current) {
        if (*current == targetPath) {
            break;
        }
    }
    for (; current != fullPath.end(); ++current) {
        result /= *current;
    }
    return result;
}

bool FileReader::ReadFiles(const fs::path& path, std::vector<ChunkFileInfo>& files,
                            const fs::path& serverPath) {
    std::error_code error;
    if (fs::is_regular_file(path, error)) {
        auto info = ReadFileInfo(path, serverPath, path.filename());
        if (info.FileId != 0) {
            files.emplace_back(std::move(info));
            return true;
        }
        return false;
    }
    if (error || !fs::is_directory(path, error)) {
        return false;
    }

    const fs::path basePath = path.filename();
    fs::recursive_directory_iterator iterator(path, error), end;
    for (; !error && iterator != end; iterator.increment(error)) {
        if (!iterator->is_regular_file(error)) {
            continue;
        }
        auto info = ReadFileInfo(iterator->path(), serverPath, CleanPath(iterator->path(), basePath));
        if (info.FileId != 0) {
            files.emplace_back(std::move(info));
        }
    }
    return !files.empty();
}

bool FileReader::ReadNextChunk(UploadStreamContext& context, ChunkFileHeader& chunk,
                               std::vector<std::uint8_t>& data) const {
    if (!context.File.is_open() || context.ChunkIndex >= context.FileInfo.ChunkCount) {
        return false;
    }
    const auto offset = static_cast<std::uint64_t>(context.ChunkIndex) * context.FileInfo.ChunkSize;
    context.File.clear();
    context.File.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!context.File) {
        return false;
    }
    data.resize(context.FileInfo.ChunkSize);
    context.File.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(data.size()));
    data.resize(static_cast<std::size_t>(context.File.gcount()));
    chunk = {context.FileInfo.FileId, context.ChunkIndex, static_cast<std::uint32_t>(data.size())};
    return chunk.Size != 0;
}

bool FileReader::ReadNextFileChunk(const ChunkFileInfo& fileInfo, ChunkFileHeader& chunk,
                                   std::ifstream& file, std::uint32_t chunkIndex,
                                   std::vector<std::uint8_t>& data) const {
    UploadStreamContext context;
    context.FileInfo = fileInfo;
    context.File = std::move(file);
    context.ChunkIndex = chunkIndex;
    const bool result = ReadNextChunk(context, chunk, data);
    file = std::move(context.File);
    return result;
}

bool FileReader::QueueNextChunk(HQUIC stream, UploadStreamContext& context,
                                PacketSender& sender) const {
    if (context.ChunkIndex >= context.FileInfo.ChunkCount ||
        (context.Transfer != nullptr &&
         context.Transfer->cancelled.load(std::memory_order_acquire))) {
        context.Finished = true;
        context.File.close();
        if (context.PendingSendChunk == 0) {
            context.Owner->api()->StreamShutdown(stream, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0);
        }
        return true;
    }

    ChunkFileHeader chunk{};
    std::vector<std::uint8_t> data;
    if (!ReadNextChunk(context, chunk, data) || !sender.SendChunk(stream, chunk, data)) {
        return false;
    }
    ++context.PendingSendChunk;
    context.ChunkIndex += context.ChunkStep;
    return true;
}

bool FileReader::QueueNextChunk(HQUIC stream, UploadStreamContext* context,
                                PacketSender& sender) const {
    return context != nullptr && QueueNextChunk(stream, *context, sender);
}

std::vector<ChunkFileInfo> FileReader::ReadFileAndDirectory(const fs::path& path,
                                                            std::vector<ChunkFileInfo>& files,
                                                            const fs::path& serverPath) {
    ReadFiles(path, files, serverPath);
    return files;
}

} // namespace FileClient

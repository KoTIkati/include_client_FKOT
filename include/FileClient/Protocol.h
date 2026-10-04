#pragma once

#include <cstdint>

namespace FileClient {

inline constexpr std::uint16_t PROTOCOL_VERSION = 1;
inline constexpr std::uint64_t MAX_PACKET_SIZE = 16ULL * 1024ULL * 1024ULL;

enum class PacketType : std::uint16_t {
    FILE_INFO = 0x01,
    FILE_CHUNK = 0x02,
    SEND_MESSAGE = 0x03,
    SEND_METADATA = 0x04,
    COMMAND_REQUEST_FILE = 0x05,
    CREATE_DIR = 0x06,
    DELETE_OBJECT = 0x07,
    RENAME = 0x08,
    MOVE_FILE = 0x09
};

enum class StreamType { UPLOAD, DOWNLOAD, COMMAND };
enum class FileInfoType { Default, Chunk, Manager };

#pragma pack(push, 1)
struct PacketHeader {
    std::uint16_t Version;
    PacketType Type;
    std::uint32_t Length;
    std::uint32_t RequestId;
};
#pragma pack(pop)

struct Packet {
    PacketHeader header{};
    const std::uint8_t* payload = nullptr;
    std::uint32_t payloadSize = 0;
};

} // namespace FileClient

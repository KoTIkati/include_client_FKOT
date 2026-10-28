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
    MOVE_FILE = 0x09,
    STOP_FILE_DOWNLOAD = 0x10,
    STOP_FILE_UPLOAD = 0x11,
    FILE_STATE = 0x12
};

enum class UploadState : std::uint8_t {
    Sending,
    AwaitingConfirm,
    Confirmed,
    Cancelled,
    Failed
};

// Направление передачи, к которой относится FILE_STATE: кто отправляет данные файла.
// Не Upload/Download, так как у сервера и клиента эти слова означают противоположное.
enum class TransferDirection : std::uint8_t {
    ClientToServer,
    ServerToClient
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

// Payload FILE_STATE: FileId + UploadState + TransferDirection
struct UploadStateSendData {
    std::uint64_t FileId;
    UploadState State;
    TransferDirection Direction;
};
#pragma pack(pop)

struct Packet {
    PacketHeader header{};
    const std::uint8_t* payload = nullptr;
    std::uint32_t payloadSize = 0;
};

} // namespace FileClient

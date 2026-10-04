#pragma once

#include <msquic.h>

namespace FileClient {

class ErrorReporter;

class MsQuic {
public:
    explicit MsQuic(ErrorReporter& reporter) noexcept;
    ~MsQuic();

    MsQuic(const MsQuic&) = delete;
    MsQuic& operator=(const MsQuic&) = delete;

    bool Initialize();
    const QUIC_API_TABLE* api() const noexcept { return api_; }
    HQUIC registration() const noexcept { return registration_; }

private:
    ErrorReporter& reporter_;
    const QUIC_API_TABLE* api_ = nullptr;
    HQUIC registration_ = nullptr;
};

} // namespace FileClient

#include <FileClient/MsQuic.h>

#include <FileClient/Error.h>

namespace FileClient {

MsQuic::MsQuic(ErrorReporter& reporter) noexcept : reporter_(reporter) {}

MsQuic::~MsQuic() {
    if (api_ != nullptr && registration_ != nullptr) {
        api_->RegistrationClose(registration_);
    }
    if (api_ != nullptr) {
        MsQuicClose(api_);
    }
}

bool MsQuic::Initialize() {
    if (api_ != nullptr && registration_ != nullptr) {
        return true;
    }

    if (MsQuicOpenVersion(QUIC_API_VERSION_2, reinterpret_cast<const void**>(&api_)) != QUIC_STATUS_SUCCESS ||
        api_ == nullptr) {
        reporter_.Report("[ERROR] MsQuicOpenVersion failed");
        api_ = nullptr;
        return false;
    }

    QUIC_REGISTRATION_CONFIG config{"file_exchange", QUIC_EXECUTION_PROFILE_LOW_LATENCY};
    const QUIC_STATUS status = api_->RegistrationOpen(&config, &registration_);
    if (QUIC_FAILED(status)) {
        reporter_.Report("[ERROR] RegistrationOpen failed");
        MsQuicClose(api_);
        api_ = nullptr;
        registration_ = nullptr;
        return false;
    }
    return true;
}

} // namespace FileClient

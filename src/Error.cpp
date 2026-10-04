#include <FileClient/Error.h>

namespace FileClient {

void ErrorReporter::SetCallback(ErrorCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = std::move(callback);
}

void ErrorReporter::Report(const std::string& message) const {
    ErrorCallback callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        callback = callback_;
    }
    if (callback) {
        callback(message);
    }
}

} // namespace FileClient

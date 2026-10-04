#pragma once

#include <functional>
#include <mutex>
#include <string>

namespace FileClient {

using ErrorCallback = std::function<void(const std::string& message)>;

class ErrorReporter {
public:
    void SetCallback(ErrorCallback callback);
    void Report(const std::string& message) const;

private:
    mutable std::mutex mutex_;
    ErrorCallback callback_;
};

} // namespace FileClient

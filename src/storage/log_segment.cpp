#include "log_segment.h"

#include <fstream>
#include <iostream>

LogSegment::LogSegment(const std::string& path)
    : filePath(path),
      base_offset(0),
      current_size(0) {
    file.open(filePath, std::ios::in | std::ios::out | std::ios::app);
    if (!file.is_open()) {
        file.open(filePath, std::ios::out | std::ios::trunc);
        file.close();
        file.open(filePath, std::ios::in | std::ios::out | std::ios::app);
    }

    file.seekg(0, std::ios::end);
    current_size = static_cast<uint64_t>(file.tellg());
}

uint64_t LogSegment::append(const std::string& message) {
    const uint64_t current_write_position = current_size;
    const uint64_t length = message.length();

    file.write(reinterpret_cast<const char*>(&length), sizeof(length));
    file.write(message.data(), static_cast<std::streamsize>(length));

    current_size += sizeof(length) + length;
    return current_write_position;
}

std::string LogSegment::read(uint64_t position) {
    file.seekg(static_cast<std::streamoff>(position));

    uint64_t length = 0;
    file.read(reinterpret_cast<char*>(&length), sizeof(length));

    std::string message(length, '\0');
    file.read(message.data(), static_cast<std::streamsize>(length));

    return message;
}

void LogSegment::flush() {
    file.flush();
}

uint64_t LogSegment::sizes() const {
    return current_size;
}

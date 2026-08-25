#include "log_segment.h"

#include <fstream>
#include <iostream>
#include <filesystem>
#include <fcntl.h>
#include <stdexcept>
#include <unistd.h>

LogSegment::LogSegment(const std::string& path)
    : filePath(path),
      base_offset(0),
      current_size(0) {
    reopen();
}

void LogSegment::reopen() {
    std::filesystem::create_directories(std::filesystem::path(filePath).parent_path());
    file.clear();
    file.open(filePath, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        file.clear();
        file.open(filePath, std::ios::out | std::ios::trunc);
        file.close();
        file.clear();
        file.open(filePath, std::ios::in | std::ios::out | std::ios::binary);
    }

    if (!file.is_open()) throw std::runtime_error("unable to open log segment: " + filePath);

    file.seekg(0, std::ios::end);
    current_size = static_cast<uint64_t>(file.tellg());
    file.clear();
    file.seekp(0, std::ios::end);
}

uint64_t LogSegment::append(const std::string& message) {
    const uint64_t current_write_position = current_size;
    const uint64_t length = message.length();

    file.clear();
    file.seekp(0, std::ios::end);
    file.write(reinterpret_cast<const char*>(&length), sizeof(length));
    file.write(message.data(), static_cast<std::streamsize>(length));

    if (!file) throw std::runtime_error("log segment write failed");
    current_size += sizeof(length) + length;
    return current_write_position;
}

std::string LogSegment::read(uint64_t position) {
    if (position > current_size || current_size - position < sizeof(uint64_t)) {
        throw std::out_of_range("invalid record position");
    }
    file.seekg(static_cast<std::streamoff>(position));

    uint64_t length = 0;
    file.read(reinterpret_cast<char*>(&length), sizeof(length));
    if (!file || length > current_size - position - sizeof(length)) {
        file.clear();
        throw std::out_of_range("incomplete record");
    }

    std::string message(length, '\0');
    file.read(message.data(), static_cast<std::streamsize>(length));
    if (!file) {
        file.clear();
        throw std::out_of_range("incomplete record");
    }

    return message;
}

void LogSegment::flush() {
    file.flush();
}

void LogSegment::sync() {
    file.flush();
    const int fd = open(filePath.c_str(), O_RDONLY);
    if (fd < 0 || fsync(fd) != 0) {
        if (fd >= 0) close(fd);
        throw std::runtime_error("segment fsync failed");
    }
    close(fd);
}

void LogSegment::truncate(uint64_t size) {
    file.close();
    std::filesystem::resize_file(filePath, size);
    current_size = size;
    reopen();
}

uint64_t LogSegment::sizes() const {
    return current_size;
}

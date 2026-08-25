#pragma once

#include <cstdint>
#include <fstream>
#include <string>

class LogSegment {
public:
    explicit LogSegment(const std::string& path);

    uint64_t append(const std::string& message);
    std::string read(uint64_t position);
    void flush();
    void sync();
    void truncate(uint64_t size);
    uint64_t sizes() const;

private:
    std::fstream file;
    std::string filePath;
    uint64_t base_offset;
    uint64_t current_size;
    void reopen();
};

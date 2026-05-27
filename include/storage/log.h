#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "log_segment.h"

struct LogIndexEntry {
    size_t segment_index;
    uint64_t byte_position;
};

class Log {
public:
    Log(const std::string& log_directory, size_t max_segment_size);

    uint64_t append(const std::string& message);
    std::string fetch(uint64_t offset);
    void flush();

private:
    void load_existing_segments();
    void rebuild_index_from_segment(size_t segment_index);

    std::string log_directory_;
    std::vector<std::unique_ptr<LogSegment>> segments;
    std::vector<LogIndexEntry> index_;
    LogSegment* active_segment;
    size_t active_segment_index_;
    uint64_t next_offset_;
    size_t max_segment_size_;
};

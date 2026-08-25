#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "log_segment.h"

struct LogIndexEntry {
    uint64_t offset;
    size_t segment_index;
    uint64_t byte_position;
};

class Log {
public:
    Log(const std::string& log_directory, size_t max_segment_size, size_t sync_batch_size);

    uint64_t append(const std::string& message);
    std::string fetch(uint64_t offset);
    void flush();

private:
    void load_existing_segments();
    void rebuild_index_from_segment(size_t segment_index);
    void write_wal(uint64_t offset, const std::string& message);
    void recover_wal();
    void clear_wal();
    void sync_wal();
    void sync_pending();

    std::string log_directory_;
    std::vector<std::unique_ptr<LogSegment>> segments;
    // Checkpoints trade a small sequential scan for dramatically lower index
    // memory use on long-lived partitions.
    std::vector<LogIndexEntry> index_;
    static constexpr uint64_t kIndexStride = 64;
    LogSegment* active_segment;
    size_t active_segment_index_;
    uint64_t next_offset_;
    size_t max_segment_size_;
    std::string wal_path_;
    size_t sync_batch_size_;
    size_t pending_appends_;
    std::mutex mutex_;
};

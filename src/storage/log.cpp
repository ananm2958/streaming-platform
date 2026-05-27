#include "log.h"

#include <algorithm>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <vector>
#include <string>

namespace fs = std::filesystem;

Log::Log(const std::string& log_directory, size_t max_segment_size)
    : log_directory_(log_directory),
      active_segment(nullptr),
      active_segment_index_(0),
      next_offset_(0),
      max_segment_size_(max_segment_size) {
    fs::create_directories(log_directory_);
    load_existing_segments();
}

void Log::load_existing_segments() {
    std::vector<fs::path> segment_paths;
    for (const auto& entry : fs::directory_iterator(log_directory_)) {
        if (!entry.is_regular_file()) {
            continue;
        }

        const std::string filename = entry.path().filename().string();
        if (filename.rfind("segment-", 0) == 0 &&
            filename.size() > 8 &&
            filename.substr(filename.size() - 4) == ".log") {
            segment_paths.push_back(entry.path());
        }
    }

    std::sort(segment_paths.begin(), segment_paths.end());

    if (segment_paths.empty()) {
        const std::string path = (fs::path(log_directory_) / "segment-0.log").string();
        segments.push_back(std::make_unique<LogSegment>(path));
        active_segment = segments.back().get();
        active_segment_index_ = 0;
        return;
    }

    for (const fs::path& path : segment_paths) {
        segments.push_back(std::make_unique<LogSegment>(path.string()));
        rebuild_index_from_segment(segments.size() - 1);
    }

    active_segment_index_ = segments.size() - 1;
    active_segment = segments[active_segment_index_].get();
    next_offset_ = index_.size();
}

void Log::rebuild_index_from_segment(size_t segment_index) {
    LogSegment* segment = segments[segment_index].get();
    uint64_t position = 0;

    while (position < segment->sizes()) {
        index_.push_back(LogIndexEntry{segment_index, position});

        const std::string message = segment->read(position);
        const uint64_t record_size =
            sizeof(uint64_t) + static_cast<uint64_t>(message.size());
        position += record_size;
    }
}

uint64_t Log::append(const std::string& message) {
    const uint64_t offset = next_offset_;
    const uint64_t byte_position = active_segment->append(message);

    index_.push_back(LogIndexEntry{active_segment_index_, byte_position});
    next_offset_++;

    if (active_segment->sizes() >= max_segment_size_) {
        const std::string path = (fs::path(log_directory_) /
                                  ("segment-" + std::to_string(segments.size()) + ".log"))
                                     .string();
        segments.push_back(std::make_unique<LogSegment>(path));
        active_segment_index_ = segments.size() - 1;
        active_segment = segments[active_segment_index_].get();
    }

    return offset;
}

std::string Log::fetch(uint64_t offset) {
    if (offset >= next_offset_) {
        throw std::out_of_range("Invalid offset");
    }

    const LogIndexEntry& entry = index_[offset];
    return segments[entry.segment_index]->read(entry.byte_position);
}

void Log::flush() {
    for (const auto& segment : segments) {
        segment->flush();
    }
}

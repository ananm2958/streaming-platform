#include "log.h"

#include <algorithm>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <vector>
#include <string>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

Log::Log(const std::string& log_directory, size_t max_segment_size, size_t sync_batch_size)
    : log_directory_(log_directory),
      active_segment(nullptr),
      active_segment_index_(0),
      next_offset_(0),
      max_segment_size_(max_segment_size),
      wal_path_((fs::path(log_directory) / "append.wal").string()),
      sync_batch_size_(std::max<size_t>(1, sync_batch_size)),
      pending_appends_(0) {
    fs::create_directories(log_directory_);
    load_existing_segments();
    recover_wal();
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
}

void Log::rebuild_index_from_segment(size_t segment_index) {
    LogSegment* segment = segments[segment_index].get();
    uint64_t position = 0;

    while (position < segment->sizes()) {
        try {
            const std::string message = segment->read(position);
            if (next_offset_ % kIndexStride == 0 || position == 0) {
                index_.push_back(LogIndexEntry{next_offset_, segment_index, position});
            }
            ++next_offset_;
            position += sizeof(uint64_t) + static_cast<uint64_t>(message.size());
        } catch (const std::exception&) {
            // A crash can leave a partially-written tail. It was never indexed
            // and is safely removed; a durable WAL entry will restore it.
            segment->truncate(position);
            break;
        }
    }
}

uint64_t Log::append(const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    const uint64_t offset = next_offset_;
    write_wal(offset, message);
    const uint64_t byte_position = active_segment->append(message);
    if (offset % kIndexStride == 0 || byte_position == 0) {
        index_.push_back(LogIndexEntry{offset, active_segment_index_, byte_position});
    }
    next_offset_++;

    if (active_segment->sizes() >= max_segment_size_) {
        const std::string path = (fs::path(log_directory_) /
                                  ("segment-" + std::to_string(segments.size()) + ".log"))
                                     .string();
        segments.push_back(std::make_unique<LogSegment>(path));
        active_segment_index_ = segments.size() - 1;
        active_segment = segments[active_segment_index_].get();
    }

    ++pending_appends_;
    if (pending_appends_ >= sync_batch_size_) sync_pending();

    return offset;
}

std::string Log::fetch(uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (offset >= next_offset_) {
        throw std::out_of_range("Invalid offset");
    }

    auto it = std::upper_bound(index_.begin(), index_.end(), offset,
        [](uint64_t value, const LogIndexEntry& entry) { return value < entry.offset; });
    if (it == index_.begin()) throw std::out_of_range("missing sparse index checkpoint");
    --it;

    uint64_t current_offset = it->offset;
    size_t segment_index = it->segment_index;
    uint64_t position = it->byte_position;
    while (true) {
        const std::string record = segments[segment_index]->read(position);
        if (current_offset == offset) return record;
        ++current_offset;
        position += sizeof(uint64_t) + record.size();
        if (position == segments[segment_index]->sizes() && segment_index + 1 < segments.size()) {
            ++segment_index;
            position = 0;
        }
        if (current_offset > offset || segment_index >= segments.size()) {
            throw std::out_of_range("invalid sparse index traversal");
        }
    }
}

void Log::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_appends_ > 0) sync_pending();
}

void Log::write_wal(uint64_t offset, const std::string& message) {
    fs::create_directories(fs::path(wal_path_).parent_path());
    std::ofstream wal(wal_path_, std::ios::binary | std::ios::app);
    if (!wal) throw std::runtime_error("unable to open WAL: " + wal_path_);
    const uint64_t length = message.size();
    wal.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    wal.write(reinterpret_cast<const char*>(&length), sizeof(length));
    wal.write(message.data(), static_cast<std::streamsize>(length));
    if (!wal) throw std::runtime_error("unable to write WAL");
}

void Log::sync_wal() {
    const int fd = open(wal_path_.c_str(), O_RDONLY);
    if (fd < 0 || fsync(fd) != 0) { if (fd >= 0) close(fd); throw std::runtime_error("WAL fsync failed"); }
    close(fd);
}

void Log::clear_wal() {
    std::ofstream wal(wal_path_, std::ios::binary | std::ios::trunc);
    wal.close();
}

void Log::sync_pending() {
    // WAL reaches stable storage before any corresponding segment data.
    sync_wal();
    for (const auto& segment : segments) segment->sync();
    clear_wal();
    pending_appends_ = 0;
}

void Log::recover_wal() {
    if (!fs::exists(wal_path_) || fs::file_size(wal_path_) == 0) return;
    std::ifstream wal(wal_path_, std::ios::binary);
    while (true) {
        uint64_t offset = 0, length = 0;
        if (!wal.read(reinterpret_cast<char*>(&offset), sizeof(offset))) break;
        if (!wal.read(reinterpret_cast<char*>(&length), sizeof(length)) || length > 16 * 1024 * 1024) break;
        std::string message(length, '\0');
        if (!wal.read(message.data(), static_cast<std::streamsize>(length))) break;
        if (offset == next_offset_) {
            const uint64_t position = active_segment->append(message);
            if (next_offset_ % kIndexStride == 0 || position == 0) {
                index_.push_back(LogIndexEntry{next_offset_, active_segment_index_, position});
            }
            ++next_offset_;
        }
    }
    if (next_offset_ > 0) for (const auto& segment : segments) segment->sync();
    clear_wal();
}

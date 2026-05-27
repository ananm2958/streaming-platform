#include "storage_engine.h"

#include "log.h"

#include <filesystem>
#include <memory>
#include <stdexcept>

namespace fs = std::filesystem;

StorageEngine::StorageEngine(
    const std::string& data_directory,
    size_t max_segment_size
)
    : data_directory_(data_directory),
      max_segment_size_(max_segment_size) {
    fs::create_directories(data_directory_);
}

StorageEngine::~StorageEngine() = default;

std::string StorageEngine::make_key(
    const std::string& topic,
    int partition
) const {
    return topic + "-" + std::to_string(partition);
}

std::string StorageEngine::log_directory_for_key(const std::string& key) const {
    return (fs::path(data_directory_) / key).string();
}

Log& StorageEngine::get_or_create_log(const std::string& key) {
    if (logs_.find(key) == logs_.end()) {
        logs_[key] = std::make_unique<Log>(
            log_directory_for_key(key),
            max_segment_size_
        );
    }

    return *logs_[key];
}

uint64_t StorageEngine::append(
    const std::string& topic,
    int partition,
    const std::string& message
) {
    const std::string key = make_key(topic, partition);
    return get_or_create_log(key).append(message);
}

std::string StorageEngine::fetch(
    const std::string& topic,
    int partition,
    uint64_t offset
) {
    const std::string key = make_key(topic, partition);

    if (logs_.find(key) == logs_.end()) {
        throw std::invalid_argument("KEY NOT FOUND");
    }

    return logs_[key]->fetch(offset);
}

void StorageEngine::flush(const std::string& topic, int partition) {
    const std::string key = make_key(topic, partition);

    if (logs_.find(key) == logs_.end()) {
        return;
    }

    logs_[key]->flush();
}

bool StorageEngine::has_log(const std::string& topic, int partition) const {
    return logs_.find(make_key(topic, partition)) != logs_.end();
}

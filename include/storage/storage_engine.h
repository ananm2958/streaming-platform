#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class Log;

class StorageEngine {
public:
    StorageEngine(const std::string& data_directory, size_t max_segment_size);
    StorageEngine(const std::string& data_directory, size_t max_segment_size,
                  size_t sync_batch_size);
    ~StorageEngine();

    uint64_t append(const std::string& topic, int partition, const std::string& message);

    std::string fetch(const std::string& topic, int partition, uint64_t offset);

    void flush(const std::string& topic, int partition);
    void flush_all();

    bool has_log(const std::string& topic, int partition) const;

    std::vector<int> partitions_for_topic(const std::string& topic) const;

private:
    std::string make_key(const std::string& topic, int partition) const;
    std::string log_directory_for_key(const std::string& key) const;
    Log& get_or_create_log(const std::string& key);

    std::string data_directory_;
    std::unordered_map<std::string, std::unique_ptr<Log>> logs_;
    size_t max_segment_size_;
    size_t sync_batch_size_;
};

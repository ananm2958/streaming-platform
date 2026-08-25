#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// Coordinates a single topic subscription per consumer group.  All methods are
// synchronized so a partition can be assigned to at most one member at a time.
class ConsumerGroupManager {
public:
    std::vector<int> join(const std::string& group, const std::string& member,
                          const std::string& topic,
                          const std::vector<int>& partitions);
    void leave(const std::string& group, const std::string& member,
               const std::vector<int>& partitions);
    bool is_assigned(const std::string& group, const std::string& member,
                     const std::string& topic, int partition) const;
    uint64_t next_offset(const std::string& group, const std::string& topic,
                         int partition) const;
    bool commit(const std::string& group, const std::string& member,
                const std::string& topic, int partition, uint64_t offset);
    void refresh_topic(const std::string& topic, const std::vector<int>& partitions);

private:
    struct Group {
        std::string topic;
        std::set<std::string> members;
        std::map<std::string, std::vector<int>> assignments;
        std::map<int, uint64_t> offsets;
    };

    static void rebalance(Group& group, const std::vector<int>& partitions);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Group> groups_;
};

#include "consumer_group_manager.h"

#include <algorithm>

void ConsumerGroupManager::rebalance(Group& group, const std::vector<int>& partitions) {
    group.assignments.clear();
    for (const std::string& member : group.members) {
        group.assignments[member] = {};
    }
    if (group.members.empty()) return;

    std::vector<int> sorted = partitions;
    std::sort(sorted.begin(), sorted.end());
    size_t index = 0;
    for (int partition : sorted) {
        auto member = group.members.begin();
        std::advance(member, index % group.members.size());
        group.assignments[*member].push_back(partition);
        ++index;
    }
}

std::vector<int> ConsumerGroupManager::join(const std::string& group_name,
                                             const std::string& member,
                                             const std::string& topic,
                                             const std::vector<int>& partitions) {
    std::lock_guard<std::mutex> lock(mutex_);
    Group& group = groups_[group_name];
    if (group.topic.empty()) group.topic = topic;
    if (group.topic != topic) return {};
    group.members.insert(member);
    rebalance(group, partitions);
    return group.assignments[member];
}

void ConsumerGroupManager::leave(const std::string& group_name, const std::string& member,
                                 const std::vector<int>& partitions) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = groups_.find(group_name);
    if (found == groups_.end()) return;
    found->second.members.erase(member);
    if (found->second.members.empty()) {
        groups_.erase(found);
        return;
    }
    rebalance(found->second, partitions);
}

bool ConsumerGroupManager::is_assigned(const std::string& group_name,
                                        const std::string& member,
                                        const std::string& topic, int partition) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto group = groups_.find(group_name);
    if (group == groups_.end() || group->second.topic != topic) return false;
    const auto assignment = group->second.assignments.find(member);
    if (assignment == group->second.assignments.end()) return false;
    return std::find(assignment->second.begin(), assignment->second.end(), partition) != assignment->second.end();
}

uint64_t ConsumerGroupManager::next_offset(const std::string& group_name,
                                            const std::string& topic, int partition) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto group = groups_.find(group_name);
    if (group == groups_.end() || group->second.topic != topic) return 0;
    const auto offset = group->second.offsets.find(partition);
    return offset == group->second.offsets.end() ? 0 : offset->second;
}

bool ConsumerGroupManager::commit(const std::string& group_name, const std::string& member,
                                  const std::string& topic, int partition, uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto group = groups_.find(group_name);
    if (group == groups_.end() || group->second.topic != topic) return false;
    const auto assignment = group->second.assignments.find(member);
    if (assignment == group->second.assignments.end() ||
        std::find(assignment->second.begin(), assignment->second.end(), partition) == assignment->second.end()) return false;
    uint64_t& next = group->second.offsets[partition];
    next = std::max(next, offset + 1);
    return true;
}

void ConsumerGroupManager::refresh_topic(const std::string& topic,
                                         const std::vector<int>& partitions) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : groups_) {
        if (entry.second.topic == topic) rebalance(entry.second, partitions);
    }
}

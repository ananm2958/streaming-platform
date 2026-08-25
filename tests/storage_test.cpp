#include "../include/storage/storage_engine.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <chrono>

int main() {
    // Uses the default 64-record / 5 ms durability policy.
    const std::string path = "/tmp/streaming-platform-storage-test";
    std::filesystem::remove_all(path);

    {
        StorageEngine storage(path, 20);
        assert(storage.append("orders", 0, "one") == 0);
        assert(storage.append("orders", 0, "two") == 1);
        assert(storage.append("orders", 0, "three") == 2);
        storage.flush("orders", 0);
    }

    const std::string benchmark_path = path + "-benchmark";
    std::filesystem::remove_all(benchmark_path);
    // Each append performs an fsync for durability, so keep this benchmark
    // short enough to run on slower local drives.
    constexpr size_t kMessages = 256;
    const std::string payload(512, 'x');
    const auto started = std::chrono::steady_clock::now();
    {
        StorageEngine storage(benchmark_path, 1024 * 1024);
        for (size_t i = 0; i < kMessages; ++i) storage.append("benchmark", 0, payload);
        storage.flush("benchmark", 0);
    }
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    std::cout << "segment write throughput: " << (kMessages * payload.size() / 1024.0 / 1024.0 / seconds) << " MiB/s\n";
    std::filesystem::remove_all(benchmark_path);

    // Rebuilding the offset index from segmented, durable records preserves
    // ordering and offsets after a process restart.
    {
        StorageEngine storage(path, 20);
        assert(storage.append("orders", 0, "four") == 3);
        assert(storage.fetch("orders", 0, 0) == "one");
        assert(storage.fetch("orders", 0, 2) == "three");
        assert(storage.fetch("orders", 0, 3) == "four");
    }

    std::filesystem::remove_all(path);
    std::cout << "storage_test passed\n";
}

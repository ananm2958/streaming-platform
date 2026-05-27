#pragma once

#include <cstdint>
#include <string>

std::string wrap_frame(const std::string& payload);

bool read_frame(int socket_fd, std::string& payload);

bool write_frame(int socket_fd, const std::string& payload);

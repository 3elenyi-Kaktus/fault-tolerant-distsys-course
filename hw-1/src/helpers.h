#pragma once

#include <arpa/inet.h>
#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <syncstream>

enum {
    BUFFER_SIZE = 1024,
    MESSAGE_HEADER_SIZE = 5
};


std::vector<std::string> split(const std::string& str, const std::string& delim);

void setSocketOption(int sock_fd, int option);

sockaddr_in fillAddress(const std::string &host, int port);

sockaddr_in fillAddress(int host, int port);

bool sendSafe(int fd, const std::string& message);

std::string recvSafe(int fd, size_t length);

bool sendMessage(int fd, const std::string& message);

std::string getMessage(int fd);

bool sendMessageTo(int fd, const std::string& message, sockaddr_in address);

std::pair<std::string, sockaddr_in> getMessageFrom(int fd, int flags = 0);
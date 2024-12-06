#include "helpers.h"

std::vector<std::string> split(const std::string& str, const std::string& delim) {
    size_t pos;
    size_t prev = 0;
    std::vector<std::string> res;
    while ((pos = str.find(delim, prev)) != std::string::npos) {
        res.push_back(std::move(str.substr(prev, pos - prev)));
        prev = pos + delim.length();
    }
    res.push_back(std::move(str.substr(prev)));
    return res;
}

void setSocketOption(int sock_fd, int option) {
    int val = 1;
    if (setsockopt(sock_fd, SOL_SOCKET, option, &val, sizeof(val))) {
        std::cerr << "Failed to set socket option\n";
    }
}

sockaddr_in fillAddress(const std::string &host, int port) {
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    if (!inet_aton(host.c_str(), &sa.sin_addr)) {
        std::cerr << "Provided address is invalid: " << host << "\n";
    }
    sa.sin_port = htons(port);
    return sa;
}

sockaddr_in fillAddress(int host, int port) {
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = host;
    sa.sin_port = htons(port);
    return sa;
}

bool sendSafe(int fd, const std::string& message) {
    size_t sent = 0;
    ssize_t n;
    while (sent < message.length()) {
        errno = 0;
        n = send(fd, message.c_str(), message.length() + 1, MSG_NOSIGNAL);
        if (n < 0) {
            std::cerr << "Error in sendMessage: " << strerror(errno) << "\n";
            return false;
        }
        sent += n;
    }
    return true;
}

std::string recvSafe(int fd, size_t length) {
    char buffer[length];
    errno = 0;
    ssize_t n = recv(fd, &buffer, length, 0);
    if (n < 0) {
        std::cerr << "Error in getMessage: " << strerror(errno) << "\n";
        throw std::runtime_error("Couldn't fulfill requested recv");
    }
    return {buffer};
}

bool sendMessage(int fd, const std::string& message) {
    size_t sent = 0;
    ssize_t n;
    std::string msg_size = std::to_string(message.length() + 1);
    std::string to_send = msg_size + std::string(MESSAGE_HEADER_SIZE - msg_size.length(), '\0') + message;
    std::osyncstream(std::cout) << "Sending: \"" << to_send << "\"\n";
    while (sent < to_send.length()) {
        errno = 0;
        n = send(fd, to_send.c_str(), to_send.length() + 1, MSG_NOSIGNAL);
        if (n < 0) {
            std::cerr << "Error in sendMessage: " << strerror(errno) << "\n";
            return false;
        }
        sent += n;
    }
    return true;
}

std::string getMessage(int fd) {
    char buffer[BUFFER_SIZE];
    errno = 0;
    ssize_t n = recv(fd, &buffer, MESSAGE_HEADER_SIZE, 0);
    if (n < 0) {
        std::cerr << "Error in getMessage: " << strerror(errno) << "\n";
        throw std::runtime_error("Couldn't read message size");
    }
    int msg_length = std::stoi(std::string(buffer, buffer + MESSAGE_HEADER_SIZE));
    std::osyncstream(std::cout) << "getMessage: length = " << msg_length << "\n";
    n = recv(fd, &buffer, msg_length, 0);
    if (n < 0) {
        std::cerr << "Error in getMessage: " << strerror(errno) << "\n";
        throw std::runtime_error("Couldn't fulfill requested recv");
    }
    std::osyncstream(std::cout) << "getMessage: \"" << std::string(buffer) << "\"\n";
    return {buffer};
}

bool sendMessageTo(int fd, const std::string& message, sockaddr_in address) {
    errno = 0;
    ssize_t n = sendto(fd, message.c_str(), message.length() + 1, MSG_NOSIGNAL, (sockaddr *) &address, sizeof(address));
    if (n < 0) {
        std::cerr << "Error in sendMessageTo: " << strerror(errno) << "\n";
        return false;
    }
    std::osyncstream(std::cout) << "sendMessageTo: \"" << std::string(message) << "\"\n";
    return true;
}

std::pair<std::string, sockaddr_in> getMessageFrom(int fd, int flags) {
    char buffer[BUFFER_SIZE];
    sockaddr_in address{};
    socklen_t address_length = sizeof(address);
    errno = 0;
    ssize_t n = recvfrom(fd, &buffer, BUFFER_SIZE, flags, (sockaddr *) &address, &address_length);
    if (n < 0) {
        std::cerr << "Error in getMessageFrom: " << strerror(errno) << "\n";
        throw std::runtime_error("Couldn't fulfill requested recvfrom");
    }
    std::osyncstream(std::cout) << "getMessageFrom: \"" << std::string(buffer) << "\"\n";
    return {buffer, address};
}
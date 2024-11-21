#include "slave.h"
#include "helpers.h"

Slave::Slave(const std::string& address) {
    host_ = address;
    std::thread servicer([&]() { pollMasterConnections(); });
    std::thread ping_poller([&]() { pollPings(); });

    findMaster();
    servicer.join();
}

void Slave::findMaster() {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    setSocketOption(sock_fd, SO_BROADCAST);
    setSocketOption(sock_fd, SO_REUSEADDR);
    sockaddr_in recv_addr = fillAddress(INADDR_ANY, BROADCAST_PORT);

    if (bind(sock_fd, (sockaddr *) &recv_addr, sizeof(recv_addr)) < 0) {
        std::cerr << "Error in BINDING\n";
        close(sock_fd);
        exit(1);
    }
    std::string message;
    sockaddr_in master_address{};
    try {
        std::tie(message, master_address) = getMessageFrom(sock_fd);
    }
    catch (...) {
        std::cerr << "Caught exception\n";
    }
    std::cout << "Received message from master: \"" << message << "\"\n";
    std::string host = message.substr(0, message.find(':'));
    int port = std::stoi(message.substr(host.length() + 1));
    std::cout << "Connecting to " << host << ":" << port << "\n";

    sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    setSocketOption(sock_fd, SO_REUSEADDR);
    master_address = fillAddress(host, port);

    std::string addr = host_ + ":" + std::to_string(REQUESTS_PORT);
    if (!sendMessageTo(sock_fd, addr, master_address)) {
        std::cout << "Error in notifying master\n";
    } else {
        std::cout << "Master is notified\n";
    }
}


void Slave::pollMasterConnections() {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        std::cout << "ERROR on creating socket\n";
        exit(1);
    }

    setSocketOption(sock_fd, SO_REUSEADDR);
    setSocketOption(sock_fd, SO_REUSEPORT);
    address_ = fillAddress(host_, REQUESTS_PORT);

    if (bind(sock_fd, (sockaddr *) &address_, sizeof(address_)) < 0) {
        std::cout << "ERROR on binding socket\n";
        exit(1);
    }

    if (listen(sock_fd, SOMAXCONN) < 0) {
        std::cout << "ERROR on listening\n";
        exit(1);
    }

    while (true) {
        sockaddr_in client_addr{};
        socklen_t init_size = sizeof(client_addr);
        std::cout << "Waiting for new connection from master\n";
        int initiator_fd = accept(sock_fd, (struct sockaddr *) &client_addr, &init_size);
        std::cout << "Accepted connection\n";
        if (initiator_fd < 0) {
            std::cout << "ERROR on accepting new client connection\n";
            exit(1);
        }
        std::thread servicer([&]() { serveRequests(initiator_fd, client_addr); });
        servicer.detach();
    }
}

void Slave::serveRequests(int fd, sockaddr_in master_address) {
    std::cout << "Start serving master connection from: " << inet_ntoa(master_address.sin_addr) << ":"
              << ntohs(master_address.sin_port) << "\n";
    std::string message;
    while (true) {
        try {
            message = getMessage(fd);
        }
        catch (...) {
            std::cerr << "Caught exception\n";
        }

        auto words = split(message, " ");
        if (words.size() != 2) {
            std::cerr << "Expected 2 vars, got: " << message << "\n";
        }
        int id = std::stoi(words[0]);
        int i = std::stoi(words[1]);
        std::cout << "Got request id: " << id << " with AT: " << i << ", reply with answer\n";
        std::string repack = std::to_string(id) + " " + std::to_string(i) + " " + std::to_string(1);
        if (!sendMessage(fd, repack)) {
            std::cerr << "Error in sending\n";
        }
    }
}


void Slave::pollPings() {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    setSocketOption(sock_fd, SO_REUSEADDR);
    sockaddr_in recv_addr = fillAddress(host_, PING_PORT);

    if (bind(sock_fd, (sockaddr *) &recv_addr, sizeof(recv_addr)) < 0) {
        std::cerr << "Error in BINDING\n";
        close(sock_fd);
        exit(1);
    }
    while (true) {
        std::string message;
        sockaddr_in pinger_address{};
        try {
            std::tie(message, pinger_address) = getMessageFrom(sock_fd);
        }
        catch (...) {
            std::cerr << "Caught exception\n";
        }
        std::cout << "Received ping from: " << inet_ntoa(pinger_address.sin_addr) << ":"
                  << ntohs(pinger_address.sin_port) << "\n";

        if (!sendMessageTo(sock_fd, std::string(), pinger_address)) {
            std::cerr << "Error in REPLYING to ping\n";
        }
    }
}

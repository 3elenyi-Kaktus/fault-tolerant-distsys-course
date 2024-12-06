#include "slave.h"
#include "helpers.h"

Slave::Slave(Config &config) {
    config_ = config;
    std::thread ping_poller([&]() {
        try {
            pollPings();
        }
        catch (...) {
            std::cerr << "Caught exception in pollPings\n";
        }
    });
    ping_poller.detach();
    std::thread servicer([&]() {
        try {
            pollMasterConnections();
        }
        catch (...) {
            std::cerr << "Caught exception in pollMasterConnections\n";
        }
    });
    while (true) {
        if (!connected_) {
            findMaster();
        } else {
            std::osyncstream(std::cout) << "Master connected, skip lookup\n";
            sleep(5);
        }
        sleep(5);
    }

    servicer.join();
}

void Slave::findMaster() {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    setSocketOption(sock_fd, SO_BROADCAST);
    setSocketOption(sock_fd, SO_REUSEADDR);
    setSocketOption(sock_fd, SO_REUSEPORT);
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
    std::osyncstream(std::cout) << "Received message from master: \"" << message << "\"\n";
    std::string host = message.substr(0, message.find(':'));
    int port = std::stoi(message.substr(host.length() + 1));
    std::osyncstream(std::cout) << "Connecting to " << host << ":" << port << "\n";

    sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    setSocketOption(sock_fd, SO_REUSEADDR);
    setSocketOption(sock_fd, SO_REUSEPORT);
    master_address = fillAddress(host, port);

    std::string addr = config_.address + ":" + std::to_string(REQUESTS_PORT);
    if (!sendMessageTo(sock_fd, addr, master_address)) {
        std::osyncstream(std::cout) << "Error in notifying master\n";
    } else {
        std::osyncstream(std::cout) << "Master is notified\n";
    }
}


void Slave::pollMasterConnections() {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        std::osyncstream(std::cout) << "ERROR on creating socket\n";
        exit(1);
    }

    setSocketOption(sock_fd, SO_REUSEADDR);
    setSocketOption(sock_fd, SO_REUSEPORT);
    address_ = fillAddress(config_.address, REQUESTS_PORT);

    if (bind(sock_fd, (sockaddr *) &address_, sizeof(address_)) < 0) {
        std::osyncstream(std::cout) << "ERROR on binding socket\n";
        exit(1);
    }

    if (listen(sock_fd, SOMAXCONN) < 0) {
        std::osyncstream(std::cout) << "ERROR on listening\n";
        exit(1);
    }

    while (true) {
        sockaddr_in client_addr{};
        socklen_t init_size = sizeof(client_addr);
        std::osyncstream(std::cout) << "Waiting for new connection from master\n";
        int initiator_fd = accept(sock_fd, (struct sockaddr *) &client_addr, &init_size);
        std::osyncstream(std::cout) << "Accepted connection\n";
        connected_ = true;
        if (initiator_fd < 0) {
            std::osyncstream(std::cout) << "ERROR on accepting new client connection\n";
            exit(1);
        }
        std::thread servicer([&, initiator_fd, client_addr]() {
            try {
                serveRequests(initiator_fd, client_addr);
            }
            catch (...) {
                std::cerr << "Caught exception in serveRequests\n";
                shutdown(initiator_fd, SHUT_RDWR);
                close(initiator_fd);
                connected_ = false;
            }
        });
        servicer.detach();
    }
}

void Slave::serveRequests(int fd, sockaddr_in master_address) {
    std::osyncstream(std::cout) << "Start serving master connection from: " << inet_ntoa(master_address.sin_addr) << ":"
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
        std::string repack = std::to_string(id) + " " + std::to_string(i) + " " + std::to_string(1);
        std::osyncstream(std::cout) << "Got request id: " << id << " with AT: " << i << ", reply with answer: "
                                    << repack << "\n";

        sleep(2); // simulate work

        if (!sendMessage(fd, repack)) {
            std::cerr << "Error in sending\n";
        }
    }
}


void Slave::pollPings() {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    setSocketOption(sock_fd, SO_REUSEADDR);
    setSocketOption(sock_fd, SO_REUSEPORT);
    sockaddr_in recv_addr = fillAddress(config_.address, PING_PORT);

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
        std::osyncstream(std::cout) << "Received ping from: " << inet_ntoa(pinger_address.sin_addr) << ":"
                                    << ntohs(pinger_address.sin_port) << "\n";

        if (random() % 100 < config_.drop_rate) {
            continue;
        }
        if (!sendMessageTo(sock_fd, std::string(), pinger_address)) {
            std::cerr << "Error in REPLYING to ping\n";
        }
    }
}

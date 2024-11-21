#include "iostream"
#include "slave.h"

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Address parameter required\n";
        return 1;
    }
    std::string slave_address(argv[1]);
    std::cout << "Slave startup at address: " << slave_address << "\n";
    Slave slave{slave_address};
    std::cout << "Terminate\n";
}
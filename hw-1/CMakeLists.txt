cmake_minimum_required(VERSION 3.25)
project(fault_tolerant_distsys_course)

set(CMAKE_CXX_STANDARD 20)

add_executable(master src/main.cpp src/master.h src/master.cpp src/slave.cpp src/slave.h src/main_slave.cpp src/helpers.cpp src/helpers.h)
add_executable(slave src/main.cpp src/master.h src/master.cpp src/slave.cpp src/slave.h src/helpers.cpp src/helpers.h)
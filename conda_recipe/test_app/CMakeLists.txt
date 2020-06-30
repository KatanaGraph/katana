cmake_minimum_required(VERSION 3.13)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Galois REQUIRED)
add_executable(test_app test_app.cpp)
target_link_libraries(test_app Galois::shmem)

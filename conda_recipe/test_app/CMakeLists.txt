cmake_minimum_required(VERSION 3.13)
project(test_app)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Katana REQUIRED)
add_executable(test_app test_app.cpp)
target_link_libraries(test_app Katana::graph)

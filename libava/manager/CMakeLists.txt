project(manager C CXX CUDA)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GLIB2 REQUIRED IMPORTED_TARGET glib-2.0)
set(CMAKE_CXX_STANDARD 11)

include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories(${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES})

find_library(NVML_LIB nvidia-ml ${CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES})
add_compile_options(-Wall -Wextra -pedantic -fpermissive -Wno-write-strings)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include ${GLIB2_INCLUDE_DIRS})
add_executable(manager manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../ava/common/socket.c
    ${gpuresc_proto_srcs}
    ${gpuresc_grpc_srcs})
target_link_libraries(manager ${GLIB2_LIBRARIES} Threads::Threads rt dl ${NVML_LIB}
)

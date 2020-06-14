project(manager C CXX CUDA)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GLIB2 REQUIRED IMPORTED_TARGET glib-2.0)
set(CMAKE_CXX_STANDARD 11)

include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories(${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES})

find_library(NVML_LIB nvidia-ml ${CMAKE_CUDA_IMPLICIT_LINK_DIRECTORIES})
add_compile_options(-Wall -Wextra -pedantic -Wno-write-strings)
include_directories(${AVA_UNPACK_LOCATION}/include ${GLIB2_INCLUDE_DIRS})
add_executable(manager
  manager.cpp
  ${AVA_UNPACK_LOCATION}/common/socket.c)
target_link_libraries(manager ${GLIB2_LIBRARIES} Threads::Threads ${NVML_LIB}
)

add_executable(spawn_daemon
  spawn_daemon.cpp
)
target_link_libraries(spawn_daemon
  ${NVML_LIB}
)

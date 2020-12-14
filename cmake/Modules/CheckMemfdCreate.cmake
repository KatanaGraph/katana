include(CheckSymbolExists)

set(CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)
check_symbol_exists(memfd_create sys/mman.h MEMFD_CREATE_FOUND)

add_executable(graph-stats graph-stats.cpp)
add_executable(graph-memory-stats graph-memory-stats.cpp)
target_link_libraries(graph-stats PRIVATE katana_galois LLVMSupport)
target_link_libraries(graph-memory-stats PRIVATE katana_galois LLVMSupport)

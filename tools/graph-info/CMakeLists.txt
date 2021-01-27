add_executable(graph-info graph-info.cpp)
target_link_libraries(graph-info PRIVATE katana_galois katana_dist_async LLVMSupport)

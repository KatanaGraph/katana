add_executable(graph-stats graph-stats.cpp)
target_link_libraries(graph-stats PRIVATE katana_graph LLVMSupport)

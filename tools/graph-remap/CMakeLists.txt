add_executable(graph-remap graph-remap.cpp)
target_link_libraries(graph-remap PRIVATE katana_graph LLVMSupport)

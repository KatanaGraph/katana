add_executable(bfs-cpu bfs_cli.cpp)
add_dependencies(apps bfs-cpu)
target_link_libraries(bfs-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 bfs-cpu INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" --edgePropertyName=value NO_VERIFY)

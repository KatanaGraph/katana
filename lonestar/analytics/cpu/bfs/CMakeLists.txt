add_executable(bfs-cpu bfs_cli.cpp)
add_dependencies(apps bfs-cpu)
target_link_libraries(bfs-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 bfs-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value NO_VERIFY)

## Test TranformView
add_test_scale(small bfs-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person)
add_test_scale(small bfs-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF)

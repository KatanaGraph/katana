add_executable(sssp-cpu sssp_cli.cpp)
add_dependencies(apps sssp-cpu)
target_link_libraries(sssp-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 sssp-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT15}" -delta=8 --edgePropertyName=value --algo=Automatic)

## Test TranformView
add_test_scale(small sssp-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person)
add_test_scale(small sssp-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF)

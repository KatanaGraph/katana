add_executable(cdlp-cpu cdlp_cli.cpp)
add_dependencies(apps cdlp-cpu)
target_link_libraries(cdlp-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small cdlp-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --algo=Synchronous)
add_test_scale(small cdlp-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15_SYMMETRIC}" -symmetricGraph --algo=Synchronous)

## Test TranformView
add_test_scale(small cdlp-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person)
add_test_scale(small cdlp-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF) 

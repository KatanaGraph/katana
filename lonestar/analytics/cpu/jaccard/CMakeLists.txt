add_executable(jaccard-cpu jaccard_cli.cpp)
add_dependencies(apps jaccard-cpu)
target_link_libraries(jaccard-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small2 jaccard-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT15_CLEANED_SYMMETRIC}" NO_VERIFY)

## Test TranformView
add_test_scale(small2 jaccard-cpu INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person NO_VERIFY)
add_test_scale(small2 jaccard-cpu INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF NO_VERIFY)

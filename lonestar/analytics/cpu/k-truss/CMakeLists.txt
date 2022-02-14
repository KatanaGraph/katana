add_executable(k-truss-cpu k_truss_cli.cpp)
add_dependencies(apps k-truss-cpu)
target_link_libraries(k-truss-cpu PRIVATE Katana::galois lonestar)

add_executable(verify-k-truss Verify.cpp)
add_dependencies(apps verify-k-truss)
target_link_libraries(verify-k-truss PRIVATE Katana::galois lonestar)

add_test_scale(small k-truss-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT10_SYMMETRIC}" NO_VERIFY -kTrussNumber=4 -symmetricGraph)
## XXX TODO(gill): Can not put more than 1 views right now: Example: Putting SortedView on Undirected view.
#add_test_scale(small k-truss-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT10}" NO_VERIFY -kTrussNumber=4)

## Test TranformView
## XXX TODO(gill): Can not put more than 1 views right now: Example: Putting SortedView on Undirected view.
#add_test_scale(small k-truss-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person -kTrussNumber=4)
#add_test_scale(small k-truss-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF -kTrussNumber=4)

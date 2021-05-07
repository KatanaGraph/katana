add_executable(k-truss-cpu k_truss_cli.cpp)
add_dependencies(apps k-truss-cpu)
target_link_libraries(k-truss-cpu PRIVATE Katana::galois lonestar)

add_executable(verify-k-truss Verify.cpp)
add_dependencies(apps verify-k-truss)
target_link_libraries(verify-k-truss PRIVATE Katana::galois lonestar)

add_test_scale(small k-truss-cpu INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat10_symmetric" NO_VERIFY -kTrussNumber=4 -symmetricGraph)

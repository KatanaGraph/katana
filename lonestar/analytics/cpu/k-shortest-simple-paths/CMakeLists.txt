add_executable(k-shortest-simple-paths-cpu yen_k_SSSP.cpp)
add_dependencies(apps k-shortest-simple-paths-cpu)
target_link_libraries(k-shortest-simple-paths-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 k-shortest-simple-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" -delta=8 --edgePropertyName=value)

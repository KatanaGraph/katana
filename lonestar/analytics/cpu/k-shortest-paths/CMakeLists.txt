add_executable(k-shortest-paths-cpu SSSP.cpp)
add_dependencies(apps k-shortest-paths-cpu)
target_link_libraries(k-shortest-paths-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" -delta=8 --edgePropertyName=value)

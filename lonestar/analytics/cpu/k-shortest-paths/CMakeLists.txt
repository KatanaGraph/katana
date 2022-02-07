add_executable(k-shortest-paths-cpu ksssp_cli.cpp)
add_dependencies(apps k-shortest-paths-cpu)
target_link_libraries(k-shortest-paths-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" -delta=8 --edgePropertyName=value)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" -delta=8)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=async --algo=DeltaTile)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=async --algo=DeltaStep)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=async --algo=DeltaStepBarrier)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=syncLevel --algo=DeltaTile)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=syncLevel --algo=DeltaStep)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=syncLevel --algo=DeltaStepBarrier)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=async --algo=DeltaTile)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=async --algo=DeltaStep)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=async --algo=DeltaStepBarrier)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=syncLevel --algo=DeltaTile)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=syncLevel --algo=DeltaStep)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=syncLevel --algo=DeltaStepBarrier)

## Test TranformView
#XXX TODO(gill): Bug in node projection
#add_test_scale(small k-shortest-paths-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person)
add_test_scale(small k-shortest-paths-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF)

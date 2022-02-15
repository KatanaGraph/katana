add_executable(k-shortest-paths-cpu ksssp_cli.cpp)
add_dependencies(apps k-shortest-paths-cpu)
target_link_libraries(k-shortest-paths-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" -delta=8 --edgePropertyName=value)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" -delta=8)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=asyncLevel --algo=DeltaTile)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=asyncLevel --algo=DeltaStep)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=asyncLevel --algo=DeltaStepBarrier)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=syncLevel --algo=DeltaTile)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=syncLevel --algo=DeltaStep)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value --reachability=syncLevel --algo=DeltaStepBarrier)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=asyncLevel --algo=DeltaTile)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=asyncLevel --algo=DeltaStep)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=asyncLevel --algo=DeltaStepBarrier)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=syncLevel --algo=DeltaTile)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=syncLevel --algo=DeltaStep)
add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --edgePropertyName=value "-symmetricGraph" --reachability=syncLevel --algo=DeltaStepBarrier)

add_executable(pagerank-cpu pagerank-cli.cpp)
add_dependencies(apps pagerank-cpu)
target_link_libraries(pagerank-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small pagerank-cpu
  INPUT rmat15 INPUT_URI "${RDG_RMAT15}" REL_TOL 0.01 MEAN_TOL 0.002
  -maxIterations=100 -algo=PushAsync)

add_test_scale(small pagerank-cpu
  INPUT rmat15 INPUT_URI "${RDG_RMAT15}" REL_TOL 0.01 MEAN_TOL 0.002
  -maxIterations=100 -algo=PushSync)

add_test_scale(small pagerank-cpu
  INPUT rmat15 INPUT_URI "${RDG_RMAT15}" REL_TOL 0.01 MEAN_TOL 0.002
  -maxIterations=100 -algo=PullTopological)

add_test_scale(small pagerank-cpu
  INPUT rmat15 INPUT_URI "${RDG_RMAT15}" REL_TOL 0.01 MEAN_TOL 0.002
  -maxIterations=100 -algo=PullResidual)

## Test TranformView
add_test_scale(small pagerank-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person)
add_test_scale(small pagerank-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF)

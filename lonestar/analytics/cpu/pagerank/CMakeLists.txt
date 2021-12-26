add_executable(pagerank-cpu pagerank-cli.cpp)
add_dependencies(apps pagerank-cpu)
target_link_libraries(pagerank-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small pagerank-cpu
  INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" REL_TOL 0.01 MEAN_TOL 0.002
  -maxIterations=100 -algo=PushAsync)

add_test_scale(small pagerank-cpu
  INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" REL_TOL 0.01 MEAN_TOL 0.002
  -maxIterations=100 -algo=PushSync)

add_test_scale(small pagerank-cpu
  INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" REL_TOL 0.01 MEAN_TOL 0.002
  -maxIterations=100 -algo=PullTopological)

add_test_scale(small pagerank-cpu
  INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" REL_TOL 0.01 MEAN_TOL 0.002
  -maxIterations=100 -algo=PullResidual)


add_executable(betweennesscentrality-cpu betweenness_centrality_cli.cpp)
add_dependencies(apps betweennesscentrality-cpu)
target_link_libraries(betweennesscentrality-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small-level betweennesscentrality-cpu
  INPUT rmat15 INPUT_URI "${RDG_RMAT15}"
  REL_TOL 0.001
  -algo=Level -numberOfSources=4 )
#add_test_scale(small-async betweennesscentrality-cpu -algo=Async -numberOfSources=4 "${RDG_RMAT15}")
add_test_scale(small-outer betweennesscentrality-cpu
  INPUT rmat15 INPUT_URI "${RDG_RMAT15}"
  REL_TOL 0.001
  -algo=Outer -numberOfSources=4 )

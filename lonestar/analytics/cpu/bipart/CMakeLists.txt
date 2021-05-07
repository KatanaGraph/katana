add_executable(bipart-cpu Helper.cpp Coarsening.cpp Partitioning.cpp Refine.cpp Metric.cpp Bipart.cpp) 
add_dependencies(apps bipart-cpu)
target_link_libraries(bipart-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 bipart-cpu INPUT ibm01 INPUT_URI "${BASEINPUT}/partitioning/ibm01.hgr" NO_VERIFY -hMetisGraph)

add_executable(pagerank-cpu pagerank-cli.cpp)
add_dependencies(apps pagerank-cpu)
target_link_libraries(pagerank-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small pagerank-cpu INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" -maxIterations=100 -algo=PushAsync)

#add_test_scale(small pagerank-cpu -transposedGraph -tolerance=0.01 "${BASEINPUT}/scalefree/transpose/rmat10.tgr")
#add_test_scale(small-topo pagerank-cpu -transposedGraph -tolerance=0.01 -algo=Topo "${BASEINPUT}/scalefree/transpose/rmat10.tgr")
#add_test_scale(small-sync pagerank-cpu -tolerance=0.01 -algo=Sync "${BASEINPUT}/propertygraphs/rmat15")

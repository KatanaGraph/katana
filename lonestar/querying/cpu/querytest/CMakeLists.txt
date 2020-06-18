add_executable(querytest querytest.cpp)
target_link_libraries(querytest galois_query)
target_compile_definitions(querytest PRIVATE GALOIS_USE_QUERY_GRAPH_TYPE=1)
add_test_scale(small querytest -isAttributedGraph -queryFile=${BASEINPUT}/ldbc/sample_queries/personlikes.query ${BASEINPUT}/ldbc/composite_0.003/graph.cygr NOT_QUICK)

add_executable(querytest querytest.cpp)
target_link_libraries(querytest galois_query)
target_compile_definitions(querytest PRIVATE GALOIS_USE_QUERY_GRAPH_TYPE=1)

# TODO add scaling as well
set(RESULT_CHECKER ${PROJECT_SOURCE_DIR}/scripts/result_checker.py)
set(query_input smalllist.txt)
# testing code
set(query_output ${BASEINPUT}/ldbc/sample_queries/003_${query_input}.truth)
if (EXISTS ${query_output})
  add_test(run-querytest ./querytest -isAttributedGraph -listOfQueries=${BASEINPUT}/ldbc/sample_queries/${query_input} ${BASEINPUT}/ldbc/composite_0.003/graph.cygr -output -outputLocation=${CMAKE_BINARY_DIR})
  add_test(verify-querytest python ${RESULT_CHECKER} -delete=1 -stringcolumn=1 ${query_output} ${CMAKE_BINARY_DIR}/queries.count)
else()
  add_test(run-querytest ./querytest -isAttributedGraph -listOfQueries=${BASEINPUT}/ldbc/sample_queries/${query_input} ${BASEINPUT}/ldbc/composite_0.003/graph.cygr)
endif()

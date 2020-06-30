add_executable(querytest querytest.cpp)
target_link_libraries(querytest galois_query)
# TODO shouldn't need to use this define; remove it in the future
target_compile_definitions(querytest PRIVATE GALOIS_USE_QUERY_GRAPH_TYPE=1)

# TODO add scaling as well
set(RESULT_CHECKER ${PROJECT_SOURCE_DIR}/scripts/result_checker.py)
set(query_input smalllist.txt)
# testing code
set(query_output ${BASEINPUT}/ldbc/sample_queries/003_${query_input}.truth)

# TODO revise testing code based on new app
#if (EXISTS ${query_output})
#  add_test(run-querytest ./querytest -isAttributedGraph -listOfQueries=${BASEINPUT}/ldbc/sample_queries/${query_input} /mnt/disk2/ldbc_003/meta -output -outputLocation=${CMAKE_BINARY_DIR})
#  add_test(verify-querytest python ${RESULT_CHECKER} -delete=1 -stringcolumn=1 ${query_output} ${CMAKE_BINARY_DIR}/queries.count)
#else()
#  add_test(run-querytest ./querytest -isAttributedGraph -listOfQueries=${BASEINPUT}/ldbc/sample_queries/${query_input} /mnt/disk2/ldbc_003/meta)
#endif()

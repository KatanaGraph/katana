add_executable(cypher-compile-test cypher-compile-test.cpp)
target_link_libraries(cypher-compile-test galois_query LLVMSupport)

add_test(NAME cypher-compile-test COMMAND cypher-compile-test)
set_tests_properties(cypher-compile-test PROPERTIES LABELS quick)

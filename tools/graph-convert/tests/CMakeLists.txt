add_executable(graph-properties-convert-test graph-properties-convert-test.cpp)
target_include_directories(graph-properties-convert-test PRIVATE ..)
target_link_libraries(graph-properties-convert-test PRIVATE LLVMSupport)
target_link_libraries(graph-properties-convert-test PRIVATE graph-properties-convert-commmon)

add_test(NAME convert-properties-graphml
  COMMAND graph-properties-convert-test --graphml ${CMAKE_CURRENT_SOURCE_DIR}/test-inputs/movies.graphml
)
set_tests_properties(convert-properties-graphml PROPERTIES LABELS quick)
add_test(NAME convert-properties-json
  COMMAND graph-properties-convert-test --json ${CMAKE_CURRENT_SOURCE_DIR}/test-inputs/movies.json
)
set_tests_properties(convert-properties-json PROPERTIES LABELS quick)
add_test(NAME convert-properties-csv
  COMMAND graph-properties-convert-test --csv ${CMAKE_CURRENT_SOURCE_DIR}/test-inputs/movies.csv
)
set_tests_properties(convert-properties-csv PROPERTIES LABELS quick)

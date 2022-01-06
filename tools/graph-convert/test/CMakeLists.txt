add_executable(unit-time-parser time-parser.cpp)
target_link_libraries(unit-time-parser PRIVATE graph-properties-convert-common)
add_test(NAME unit-time-parser COMMAND unit-time-parser)
set_tests_properties(unit-time-parser PROPERTIES LABELS quick)

add_executable(graph-properties-convert-test graph-properties-convert-test.cpp)
target_link_libraries(graph-properties-convert-test PRIVATE LLVMSupport)
target_link_libraries(graph-properties-convert-test PRIVATE LibXml2::LibXml2)
target_link_libraries(graph-properties-convert-test PRIVATE graph-properties-convert-common)

set(inputs ${MISC_TEST_DATASETS}/graph-convert)

add_test(NAME convert-properties-graphml
  COMMAND graph-properties-convert-test --neo4j --movies ${inputs}/movies.graphml
)
set_tests_properties(convert-properties-graphml PROPERTIES LABELS quick)

add_test(NAME convert-properties-graphml-types
  COMMAND graph-properties-convert-test --neo4j --types ${inputs}/array_test.graphml
)
set_tests_properties(convert-properties-graphml-types PROPERTIES LABELS quick)

add_test(NAME convert-properties-graphml-chunks
  COMMAND graph-properties-convert-test --neo4j --chunks --chunkSize 3 ${inputs}/array_test.graphml
)
set_tests_properties(convert-properties-graphml-chunks PROPERTIES LABELS quick)

if(mongoc-1.0_FOUND)
  add_test(NAME convert-properties-mongodb
    COMMAND graph-properties-convert-test --mongodb --mongo friend
  )
  set_tests_properties(convert-properties-mongodb PROPERTIES LABELS quick)
else()
  message(STATUS "Skipping mongodb tests")
endif()

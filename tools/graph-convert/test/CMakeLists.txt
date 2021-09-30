add_executable(unit-time-parser time-parser.cpp)
target_link_libraries(unit-time-parser PRIVATE graph-properties-convert-common)
add_test(NAME unit-time-parser COMMAND unit-time-parser)
set_common_katana_test_options(unit-time-parser)

add_executable(graph-properties-convert-test graph-properties-convert-test.cpp)
target_link_libraries(graph-properties-convert-test PRIVATE LLVMSupport)
target_link_libraries(graph-properties-convert-test PRIVATE LibXml2::LibXml2)
target_link_libraries(graph-properties-convert-test PRIVATE graph-properties-convert-common)

add_test(NAME convert-properties-graphml
  COMMAND graph-properties-convert-test --neo4j --movies ${CMAKE_CURRENT_SOURCE_DIR}/../test-inputs/movies.graphml
)
set_common_katana_test_options(convert-properties-graphml)

add_test(NAME convert-properties-graphml-types
  COMMAND graph-properties-convert-test --neo4j --types ${CMAKE_CURRENT_SOURCE_DIR}/../test-inputs/array_test.graphml
)
set_common_katana_test_options(convert-properties-graphml-types)

add_test(NAME convert-properties-graphml-chunks
  COMMAND graph-properties-convert-test --neo4j --chunks --chunkSize 3 ${CMAKE_CURRENT_SOURCE_DIR}/../test-inputs/array_test.graphml
)
set_common_katana_test_options(convert-properties-graphml-chunks)

if(mongoc-1.0_FOUND)
  add_test(NAME convert-properties-mongodb
    COMMAND graph-properties-convert-test --mongodb --mongo friend
  )
  set_common_katana_test_options(convert-properties-mongodb)
else()
  message(STATUS "Skipping mongodb tests")
endif()

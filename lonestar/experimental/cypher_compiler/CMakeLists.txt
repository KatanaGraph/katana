set(CYPHER_PARSER_ROOT "${CMAKE_SOURCE_DIR}/libcypher-parser")
set(CYPHER_PARSER_INCLUDE "${CYPHER_PARSER_ROOT}/lib/src")
set(CYPHER_PARSER_LIBRARY "${CYPHER_PARSER_ROOT}/lib/src/.libs/libcypher-parser.a")
add_custom_command(
  OUTPUT ${CYPHER_PARSER_LIBRARY}
  WORKING_DIRECTORY ${CYPHER_PARSER_ROOT}
  COMMAND ./autogen.sh
  COMMAND ./configure
  COMMAND make 
)
add_custom_target(cypher_parser DEPENDS ${CYPHER_PARSER_LIBRARY})

add_executable(cypher_compiler CypherCompiler.cpp)
add_dependencies(cypher_compiler cypher_parser)
target_link_libraries(cypher_compiler ${CYPHER_PARSER_LIBRARY})
target_include_directories(cypher_compiler PUBLIC ${CYPHER_PARSER_INCLUDE})

set(CYPHER_PARSER_ROOT "${CMAKE_SOURCE_DIR}/external/libcypher-parser")
set(CYPHER_PARSER_INCLUDE "${CYPHER_PARSER_ROOT}/lib/src")
set(CYPHER_PARSER_LIBRARY "${CYPHER_PARSER_ROOT}/lib/src/.libs/libcypher-parser.a")
add_custom_command(
  OUTPUT ${CYPHER_PARSER_LIBRARY}
  WORKING_DIRECTORY ${CYPHER_PARSER_ROOT}
  COMMAND ./autogen.sh
  COMMAND ./configure CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CFLAGS='-fPIC'
  COMMAND make
)
add_custom_target(cypher_parser DEPENDS ${CYPHER_PARSER_LIBRARY})

add_library(galois_query SHARED 
  src/AttributedGraph.cpp
  src/CypherCompiler.cpp
  src/GraphSimulation.cpp
  src/LDBCReader.cpp
)

target_compile_definitions(galois_query PRIVATE GALOIS_USE_QUERY_GRAPH_TYPE=1)
target_compile_definitions(galois_query PRIVATE USE_QUERY_GRAPH_WITH_NODE_LABEL=1)

target_include_directories(galois_query PUBLIC 
  ${CMAKE_SOURCE_DIR}/libgalois/include
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(galois_query cypher_parser)
target_link_libraries(galois_query ${Boost_SERIALIZATION_LIBRARY_RELEASE} lonestar pangolin ${CYPHER_PARSER_LIBRARY})
target_include_directories(galois_query PUBLIC ${CYPHER_PARSER_INCLUDE})

if (CMAKE_HAVE_PTHREAD_H)
   add_definitions(-DGALOIS_HAVE_PTHREAD)
   target_link_libraries(galois_query Threads::Threads)
endif()

find_package(NUMA)
if(NUMA_FOUND)
  add_definitions(-DGALOIS_USE_NUMA)
  target_link_libraries(galois_query ${NUMA_LIBRARY})
else()
  message(WARNING "No NUMA Support.  Likely poor performance")
endif()

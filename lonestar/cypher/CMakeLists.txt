set(CYPHER_PARSER_ROOT "${CMAKE_SOURCE_DIR}/libcypher-parser")
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

add_library(cypher_galois SHARED 
  PythonGraph.cpp
  PythonGraphReport.cpp
  PythonGraphSimulation.cpp
  PythonGraphQuery.cpp
  GraphSimulation.cpp
  $<TARGET_OBJECTS:galois_shmem_obj> 
  $<TARGET_OBJECTS:gllvm_obj> 
)

target_include_directories(cypher_galois PUBLIC 
  ${CMAKE_SOURCE_DIR}/libgalois/include
  ${CMAKE_SOURCE_DIR}/libgalois/experimental/include
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(cypher_galois ${Boost_SERIALIZATION_LIBRARY_RELEASE} lonestar)
set_target_properties (cypher_galois PROPERTIES 
  INTERFACE_POSITION_INDEPENDENT_CODE On 
  POSITION_INDEPENDENT_CODE On
)

add_dependencies(cypher_galois cypher_parser)
target_link_libraries(cypher_galois ${CYPHER_PARSER_LIBRARY})
target_include_directories(cypher_galois PUBLIC ${CYPHER_PARSER_INCLUDE})

if (USE_VTUNE)
  find_package(VTune)
  if (VTune_FOUND)
    target_link_libraries(cypher_galois ${VTune_LIBRARIES})
  endif()
endif()

if (CMAKE_HAVE_PTHREAD_H)
   add_definitions(-DGALOIS_HAVE_PTHREAD)
   target_link_libraries(cypher_galois Threads::Threads)
endif()

find_package(NUMA)
if(NUMA_FOUND)
  add_definitions(-DGALOIS_USE_NUMA)
  target_link_libraries(cypher_galois ${NUMA_LIBRARY})
else()
  message(WARNING "No NUMA Support.  Likely poor performance")
endif()

#app(python_graph_test test.cpp)
#target_include_directories(python_graph_test PUBLIC 
#  ${CMAKE_SOURCE_DIR}/libgalois/include
#  ${CMAKE_SOURCE_DIR}/libgalois/experimental/include
#  ${CMAKE_CURRENT_SOURCE_DIR}
#)
#target_link_libraries(python_graph_test cypher_galois)

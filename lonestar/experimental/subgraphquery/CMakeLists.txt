app(subgraph_query SubgraphQuery.cpp)

target_include_directories(subgraph_query PUBLIC
  ${CMAKE_SOURCE_DIR}/lonestar/experimental/python
  ${CMAKE_SOURCE_DIR}/lonestar/experimental/graphsimulation
)

target_link_libraries(subgraph_query galois_python)

add_executable(subgraph-extraction-cpu subgraph_extraction_cli.cpp)
add_dependencies(apps subgraph-extraction-cpu)
target_link_libraries(subgraph-extraction-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 subgraph-extraction-cpu INPUT rmat10 INPUT_URI
  "${RDG_RMAT10}" "--nodes=0 3 11 120" NO_VERIFY)

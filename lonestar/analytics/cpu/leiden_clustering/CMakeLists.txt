add_executable(leiden-clustering-cpu leiden_clustering_cli.cpp)
add_dependencies(apps leiden-clustering-cpu)
target_link_libraries(leiden-clustering-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small leiden-clustering-cpu NO_VERIFY INPUT rmat10 INPUT_URI "${RDG_RMAT10_SYMMETRIC}" "-symmetricGraph" --edgePropertyName=value --algo=DoAll)
add_test_scale(small leiden-clustering-cpu NO_VERIFY INPUT rmat10 INPUT_URI "${RDG_RMAT10_SYMMETRIC}" "-symmetricGraph" --edgePropertyName=value --algo=Deterministic)

add_test_scale(small leiden-clustering-cpu NO_VERIFY INPUT rmat10 INPUT_URI "${RDG_RMAT10}" --edgePropertyName=value --algo=DoAll)
add_test_scale(small leiden-clustering-cpu NO_VERIFY INPUT rmat10 INPUT_URI "${RDG_RMAT10}" --edgePropertyName=value --algo=Deterministic)

## Test TranformView
add_test_scale(small leiden-clustering-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person --algo=Deterministic) 
add_test_scale(small leiden-clustering-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF --algo=Deterministic) 

add_executable(leiden-clustering-cpu leiden_clustering_cli.cpp)
add_dependencies(apps leiden-clustering-cpu)
target_link_libraries(leiden-clustering-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small leiden-clustering-cpu NO_VERIFY INPUT rmat10 INPUT_URI "${BASEINPUT}/propertygraphs/rmat10_symmetric" "-symmetricGraph" --edgePropertyName=value --algo=DoAll) 
add_test_scale(small leiden-clustering-cpu NO_VERIFY INPUT rmat10 INPUT_URI "${BASEINPUT}/propertygraphs/rmat10_symmetric" "-symmetricGraph" --edgePropertyName=value --algo=Deterministic) 

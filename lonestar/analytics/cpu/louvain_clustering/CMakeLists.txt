add_executable(louvain-clustering-cpu louvain_clustering_cli.cpp)
add_dependencies(apps louvain-clustering-cpu)
target_link_libraries(louvain-clustering-cpu PRIVATE Katana::galois lonestar)
install(TARGETS louvain-clustering-cpu DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT apps EXCLUDE_FROM_ALL)
add_test_scale(small louvain-clustering-cpu NO_VERIFY INPUT rmat10 INPUT_URI "${BASEINPUT}/propertygraphs/rmat10_symmetric" "-symmetricGraph" --edgePropertyName=value) 


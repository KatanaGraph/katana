# Avoid collisions with existing application targets with the same name.
# The expected public names (without _) are provided using pure python modules.
add_cython_target(_bfs_property_graph _bfs_property_graph.pyx CXX OUTPUT_VAR BFSPG_SOURCES)
add_library(_bfs_property_graph MODULE ${BFSPG_SOURCES})
python_extension_module(_bfs_property_graph)
add_dependencies(_bfs_property_graph graphs)
target_link_libraries(_bfs_property_graph Galois::shmem)

install(
  TARGETS _bfs_property_graph
  LIBRARY DESTINATION python/galois/lonestar/analytics
)

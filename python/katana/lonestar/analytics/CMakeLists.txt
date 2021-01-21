# Avoid collisions with existing application targets with the same name.
# The expected public names (without _) are provided using pure python modules.
add_cython_module(_bfs_property_graph
    SOURCES _bfs_property_graph.pyx
    DEPENDS graphs
    LIBRARIES Katana::galois)


install(
  TARGETS _bfs_property_graph
  LIBRARY DESTINATION python/katana/lonestar/analytics
)

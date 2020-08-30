# Avoid collisions with existing application targets with the same name.
# The expected public names (without _) are provided using pure python modules.
add_cython_target(_bfs _bfs.pyx CXX OUTPUT_VAR BFS_SOURCES)
add_library(_bfs MODULE ${BFS_SOURCES})
python_extension_module(_bfs)
add_dependencies(_bfs graphs)
target_link_libraries(_bfs Galois::shmem)

add_cython_target(_sssp _sssp.pyx shmem.pyx CXX OUTPUT_VAR SSSP_SOURCES)
add_library(_sssp MODULE ${SSSP_SOURCES})
python_extension_module(_sssp)
add_dependencies(_sssp graphs)
target_link_libraries(_sssp Galois::shmem)

add_cython_target(_pagerank _pagerank.pyx CXX OUTPUT_VAR PAGERANK_SOURCES)
add_library(_pagerank MODULE ${PAGERANK_SOURCES})
python_extension_module(_pagerank)
add_dependencies(_pagerank graphs)
target_link_libraries(_pagerank Galois::shmem)

add_cython_target(_jaccard _jaccard.pyx CXX OUTPUT_VAR JACCARD_SOURCES)
add_library(_jaccard MODULE ${JACCARD_SOURCES})
python_extension_module(_jaccard)
add_dependencies(_jaccard graphs)
target_link_libraries(_jaccard Galois::shmem)

add_cython_target(_connected_components _connected_components.pyx shmem.pyx CXX OUTPUT_VAR CONNECTED_COMPONENTS_SOURCES)
add_library(_connected_components MODULE ${CONNECTED_COMPONENTS_SOURCES})
python_extension_module(_connected_components)
add_dependencies(_connected_components graphs)
target_link_libraries(_connected_components Galois::shmem)

install(
  TARGETS _bfs _pagerank _jaccard _sssp _connected_components
  LIBRARY DESTINATION python/galois/lonestar/gr/analytics
)

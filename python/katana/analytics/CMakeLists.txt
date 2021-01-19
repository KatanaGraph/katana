# Avoid collisions with existing application targets with the same name.
# The expected public names (without _) are provided using pure python modules.

add_cython_target(plan plan.pyx CXX OUTPUT_VAR PLAN_SOURCES)
add_library(plan MODULE ${PLAN_SOURCES})
python_extension_module(plan)
target_link_libraries(plan Katana::galois)

add_cython_target(_wrappers _wrappers.pyx CXX OUTPUT_VAR WRAPPERS_SOURCES)
add_library(_wrappers MODULE ${WRAPPERS_SOURCES})
python_extension_module(_wrappers)
add_dependencies(_wrappers plan)
target_link_libraries(_wrappers Katana::galois)

add_cython_target(_pagerank _pagerank.pyx CXX OUTPUT_VAR PAGERANK_SOURCES)
add_library(_pagerank MODULE ${PAGERANK_SOURCES})
python_extension_module(_pagerank)
add_dependencies(_pagerank plan)
target_link_libraries(_pagerank Katana::galois)

add_cython_target(_betweenness_centrality _betweenness_centrality.pyx CXX OUTPUT_VAR BETWEENNESS_CENTRALITY_SOURCES)
add_library(_betweenness_centrality MODULE ${BETWEENNESS_CENTRALITY_SOURCES})
python_extension_module(_betweenness_centrality)
add_dependencies(_betweenness_centrality plan)
target_link_libraries(_betweenness_centrality Katana::galois)

add_cython_target(_triangle_count _triangle_count.pyx CXX OUTPUT_VAR TRIANGLE_COUNT_SOURCES)
add_library(_triangle_count MODULE ${TRIANGLE_COUNT_SOURCES})
python_extension_module(_triangle_count)
add_dependencies(_triangle_count plan)
target_link_libraries(_triangle_count Katana::galois)

add_cython_target(_independent_set _independent_set.pyx CXX OUTPUT_VAR INDEPENDENT_SET_SOURCES)
add_library(_independent_set MODULE ${INDEPENDENT_SET_SOURCES})
python_extension_module(_independent_set)
add_dependencies(_independent_set plan)
target_link_libraries(_independent_set Katana::galois)

install(
  TARGETS _wrappers _pagerank _betweenness_centrality _triangle_count _independent_set plan
  LIBRARY DESTINATION python/katana/analytics
)

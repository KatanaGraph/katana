add_library(katana_graph)
add_library(Katana::graph ALIAS katana_graph)
set_target_properties(katana_graph PROPERTIES EXPORT_NAME graph)
add_dependencies(lib katana_graph)

set(sources
        src/BuildGraph.cpp
        src/FileGraph.cpp
        src/FileGraphParallel.cpp
        src/GraphHelpers.cpp
        src/GraphML.cpp
        src/GraphMLSchema.cpp
        src/GraphTopology.cpp
        src/OCFileGraph.cpp
        src/Properties.cpp
        src/PropertyGraph.cpp
        src/PropertyGraphRetractor.cpp
        src/EntityIndex.cpp
        src/PropertyViews.cpp
        src/SharedMemSys.cpp
        src/TopologyGeneration.cpp
        src/TransformationView.cpp
        src/analytics/Utils.cpp
        src/analytics/betweenness_centrality/betweenness_centrality.cpp
        src/analytics/betweenness_centrality/level.cpp
        src/analytics/betweenness_centrality/outer.cpp
        src/analytics/bfs/bfs.cpp
        src/analytics/cdlp/cdlp.cpp
        src/analytics/connected_components/connected_components.cpp
        src/analytics/independent_set/independent_set.cpp
        src/analytics/jaccard/jaccard.cpp
        src/analytics/k_core/k_core.cpp
        src/analytics/k_shortest_paths/ksssp.cpp
        src/analytics/k_truss/k_truss.cpp
        src/analytics/pagerank/pagerank-pull.cpp
        src/analytics/pagerank/pagerank-push.cpp
        src/analytics/pagerank/pagerank.cpp
        src/analytics/sssp/sssp.cpp
        src/analytics/triangle_count/triangle_count.cpp
        src/analytics/louvain_clustering/louvain_clustering.cpp
        src/analytics/random_walks/random_walks.cpp
        src/analytics/local_clustering_coefficient/local_clustering_coefficient.cpp
        src/analytics/subgraph_extraction/subgraph_extraction.cpp
        src/analytics/leiden_clustering/leiden_clustering.cpp
        src/analytics/matrix_completion/matrix_completion.cpp
    )

find_package(LibXml2 2.9.1 REQUIRED)

target_sources(katana_graph PRIVATE ${sources})

target_include_directories(katana_graph PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

target_link_libraries(katana_graph PUBLIC katana_tsuba)
target_link_libraries(katana_graph PUBLIC katana_galois)
target_link_libraries(katana_graph PUBLIC katana_support)
target_link_libraries(katana_graph PUBLIC LibXml2::LibXml2)

set_common_katana_library_options(katana_graph)

if(KATANA_IS_MAIN_PROJECT AND BUILD_TESTING)
  add_subdirectory(test)
endif()

install(
  DIRECTORY include/
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT dev
  FILES_MATCHING PATTERN "*.h"
)

install(
  TARGETS katana_graph
  EXPORT KatanaTargets
  LIBRARY
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT shlib
  ARCHIVE
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT lib
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

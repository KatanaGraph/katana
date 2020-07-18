include_directories(..)
include_directories(${NumPy_INCLUDE_DIR})

# A function for generating the complex Cython files easily
set(SOURCE_DIRECTORY_GENERATED_FILE_WARNING "in the SOURCE DIRECTORY! (This is to work around limitations in the Cython cmake interface.)")
# TODO: Avoid this messy "generated code in the source directory" issue.
message(STATUS "Generating pxd/pyx files ${SOURCE_DIRECTORY_GENERATED_FILE_WARNING}")

set(GENERATE_FROM_JINJA ${CMAKE_CURRENT_SOURCE_DIR}/../generate-from-jinja.py)

function(generate_cython_file FILE)
  set(options)
  set(options1)
  set(optionsmany DEPENDS)
  cmake_parse_arguments(_args "${options}" "${options1}" "${optionsmany}" ${ARGN})
  add_custom_command(
    COMMAND ${PYTHON_EXECUTABLE} ${GENERATE_FROM_JINJA} ${CMAKE_CURRENT_SOURCE_DIR} ${FILE}.jinja > ${CMAKE_CURRENT_SOURCE_DIR}/${FILE}
    DEPENDS ${FILE}.jinja ${_args_DEPENDS}
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/${FILE}
    COMMENT "Generating ${FILE} ${SOURCE_DIRECTORY_GENERATED_FILE_WARNING}"
    )
endfunction()

function(generate_cython_source BASE)
  set(options)
  set(options1)
  set(optionsmany DEPENDS)
  cmake_parse_arguments(_args "${options}" "${options1}" "${optionsmany}" ${ARGN})
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${BASE}.pxd.jinja)
    generate_cython_file(${BASE}.pxd DEPENDS ${_args_DEPENDS})
    # HACK: Explicitly install the generated pxd since the Cython target will not pick it up.
    install(
      FILES ${BASE}.pxd
      DESTINATION python/galois
      )
    add_custom_target (${BASE}_pxd DEPENDS ${BASE}.pxd)
  endif()
  generate_cython_file(${BASE}.pyx DEPENDS ${_args_DEPENDS})
endfunction()

add_cython_target(shmem shmem.pyx CXX OUTPUT_VAR SHMEM_SOURCES)
add_library(shmem MODULE ${SHMEM_SOURCES})
python_extension_module(shmem)
target_link_libraries(shmem Galois::shmem)

generate_cython_source(graphs DEPENDS graphs.instantiations.jinja)
add_cython_target(graphs graphs.pyx CXX OUTPUT_VAR GRAPHS_SOURCES)
add_library(graphs MODULE ${GRAPHS_SOURCES})
python_extension_module(graphs)
add_dependencies(graphs graphs_pxd)
target_link_libraries(graphs Galois::shmem)

generate_cython_source(_loops DEPENDS)
add_cython_target(_loops _loops.pyx CXX OUTPUT_VAR _LOOPS_SOURCES)
add_library(_loops MODULE ${_LOOPS_SOURCES})
python_extension_module(_loops)
add_dependencies(_loops _loops_pxd)
target_link_libraries(_loops Galois::shmem)

add_cython_target(property_graph property_graph.pyx CXX OUTPUT_VAR PROPERTYGRAPH_SOURCES)
add_library(property_graph MODULE ${PROPERTYGRAPH_SOURCES})
python_extension_module(property_graph)
add_dependencies(property_graph graphs)
target_link_libraries(property_graph Galois::shmem)

add_cython_target(timer timer.pyx CXX OUTPUT_VAR TIMER_SOURCES)
add_library(timer MODULE ${TIMER_SOURCES})
python_extension_module(timer)
target_link_libraries(timer Galois::shmem)

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

add_cython_target(_bfs_property_graph _bfs_property_graph.pyx CXX OUTPUT_VAR BFSPG_SOURCES)
add_library(_bfs_property_graph MODULE ${BFSPG_SOURCES})
python_extension_module(_bfs_property_graph)
add_dependencies(_bfs_property_graph graphs)
target_link_libraries(_bfs_property_graph Galois::shmem)

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

add_subdirectory(numba)

install(
  TARGETS shmem graphs timer _loops _bfs _pagerank _jaccard _sssp _connected_components property_graph _bfs_property_graph
  LIBRARY DESTINATION python/galois
)

if(BUILD_LIBGALOIS)
  install(
    TARGETS galois_shmem
    PUBLIC_HEADER DESTINATION include
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    INCLUDES DESTINATION include
  )

  # In order to preserve the directory structure, galois_shmem uses
  # install(DIRECTORY) rather than setting the PUBLIC_HEADER property on the
  # galois_shmem itself. Mirror the install(DIRECTORY) logic here.
  get_target_property(GALOIS_SUPPORT_SOURCE_DIR galois_support SOURCE_DIR)
  get_target_property(GALOIS_SUPPORT_BINARY_DIR galois_support BINARY_DIR)
  install(
    DIRECTORY "${GALOIS_SUPPORT_SOURCE_DIR}/include/" "${GALOIS_SUPPORT_BINARY_DIR}/include/"
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
  )
  get_target_property(TSUBA_SOURCE_DIR tsuba SOURCE_DIR)
  get_target_property(TSUBA_BINARY_DIR tsuba BINARY_DIR)
  install(
    DIRECTORY "${TSUBA_SOURCE_DIR}/include/" "${TSUBA_BINARY_DIR}/include/"
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
  )
  get_target_property(GALOIS_SOURCE_DIR galois_shmem SOURCE_DIR)
  get_target_property(GALOIS_BINARY_DIR galois_shmem BINARY_DIR)
  install(
    DIRECTORY "${GALOIS_SOURCE_DIR}/include/" "${GALOIS_BINARY_DIR}/include/"
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
  )
  get_target_property(PYGALOIS_SOURCE_DIR pygalois INTERFACE_INCLUDE_DIRECTORIES)
  install(
    DIRECTORY "${PYGALOIS_SOURCE_DIR}"
    DESTINATION .
    FILES_MATCHING PATTERN "*.h"
  )
endif()

include_directories(..)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/..)
include_directories(${NumPy_INCLUDE_DIR})

set(GENERATE_FROM_JINJA ${CMAKE_CURRENT_SOURCE_DIR}/../generate_from_jinja.py)
set(GENERATOR_CONF ${CMAKE_CURRENT_SOURCE_DIR}/../generator_conf.py)

# A function for generating the complex Cython files easily
function(generate_cython_file FILE)
  set(options)
  set(options1)
  set(optionsmany DEPENDS)
  cmake_parse_arguments(_args "${options}" "${options1}" "${optionsmany}" ${ARGN})
  add_custom_command(
    COMMAND ${PYTHON_EXECUTABLE} ${GENERATE_FROM_JINJA} ${CMAKE_CURRENT_SOURCE_DIR} ${FILE}.jinja > ${CMAKE_CURRENT_BINARY_DIR}/${FILE}
    DEPENDS ${FILE}.jinja ${_args_DEPENDS} ${GENERATE_FROM_JINJA} ${GENERATOR_CONF}
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${FILE}
    COMMENT "Generating ${CMAKE_CURRENT_BINARY_DIR}/${FILE}"
    )
  set_source_files_properties(${CMAKE_CURRENT_BINARY_DIR}/${FILE} PROPERTIES GENERATED TRUE)
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
      FILES ${CMAKE_CURRENT_BINARY_DIR}/${BASE}.pxd
      DESTINATION python/katana
      )
    add_custom_target (${BASE}_pxd DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${BASE}.pxd)
  endif()
  generate_cython_file(${BASE}.pyx DEPENDS ${_args_DEPENDS})
endfunction()

add_cython_target(galois galois.pyx CXX OUTPUT_VAR GALOIS_SOURCES)
add_library(galois MODULE ${GALOIS_SOURCES})
python_extension_module(galois)
target_link_libraries(galois Katana::galois)

add_cython_target(numpy_structs numpy_structs.pyx CXX OUTPUT_VAR NUMPY_STRUCTS_SOURCES)
add_library(numpy_structs MODULE ${NUMPY_STRUCTS_SOURCES})
python_extension_module(numpy_structs)

generate_cython_source(graphs DEPENDS graphs.instantiations.jinja)
add_cython_target(graphs ${CMAKE_CURRENT_BINARY_DIR}/graphs.pyx CXX OUTPUT_VAR GRAPHS_SOURCES)
add_library(graphs MODULE ${GRAPHS_SOURCES})
python_extension_module(graphs)
add_dependencies(graphs galois graphs_pxd)
target_link_libraries(graphs Katana::galois)

generate_cython_source(atomic DEPENDS numba_wrapper_support.pyx.jinja)
add_cython_target(atomic ${CMAKE_CURRENT_BINARY_DIR}/atomic.pyx CXX OUTPUT_VAR ATOMIC_SOURCES)
add_library(atomic MODULE ${ATOMIC_SOURCES})
python_extension_module(atomic)
add_dependencies(atomic galois)
target_link_libraries(atomic Katana::galois)

generate_cython_source(datastructures DEPENDS numba_wrapper_support.pyx.jinja)
add_cython_target(datastructures ${CMAKE_CURRENT_BINARY_DIR}/datastructures.pyx CXX OUTPUT_VAR DATASTRUCTURES_SOURCES)
add_library(datastructures MODULE ${DATASTRUCTURES_SOURCES})
python_extension_module(datastructures)
add_dependencies(datastructures galois numpy_structs)
target_link_libraries(datastructures Katana::galois)

generate_cython_source(property_graph DEPENDS numba_wrapper_support.pyx.jinja)
add_cython_target(property_graph ${CMAKE_CURRENT_BINARY_DIR}/property_graph.pyx CXX OUTPUT_VAR PROPERTYGRAPH_SOURCES)
add_library(property_graph MODULE ${PROPERTYGRAPH_SOURCES})
python_extension_module(property_graph)
add_dependencies(property_graph graphs property_graph_pxd)
target_link_libraries(property_graph Katana::galois)

generate_cython_source(_loops DEPENDS numba_wrapper_support.pyx.jinja)
add_cython_target(_loops ${CMAKE_CURRENT_BINARY_DIR}/_loops.pyx CXX OUTPUT_VAR _LOOPS_SOURCES)
add_library(_loops MODULE ${_LOOPS_SOURCES})
python_extension_module(_loops)
add_dependencies(_loops galois numpy_structs datastructures_pxd property_graph_pxd)
target_link_libraries(_loops Katana::galois)

add_cython_target(timer timer.pyx CXX OUTPUT_VAR TIMER_SOURCES)
add_library(timer MODULE ${TIMER_SOURCES})
python_extension_module(timer)
target_link_libraries(timer Katana::galois)

# Symlink all python source file into binary directory so that documentation generation can load python modules correctly.
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/analytics)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lonestar/analytics)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/numba_support)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/util)
file(GLOB_RECURSE PY_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.py)
foreach(X IN LISTS PY_FILES)
    file(CREATE_LINK ${CMAKE_CURRENT_SOURCE_DIR}/${X} ${CMAKE_CURRENT_BINARY_DIR}/${X} SYMBOLIC)
endforeach()

add_subdirectory(numba_support)
add_subdirectory(lonestar/analytics)
add_subdirectory(analytics)

install(
  TARGETS galois numpy_structs graphs timer atomic datastructures _loops property_graph
  LIBRARY DESTINATION python/katana
)

if(BUILD_LIBGALOIS)
  install(
    TARGETS katana_galois tsuba tsuba-preload
    PUBLIC_HEADER DESTINATION include
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    INCLUDES DESTINATION include
  )

  # In order to preserve the directory structure, katana_galois uses
  # install(DIRECTORY) rather than setting the PUBLIC_HEADER property on the
  # katana_galois itself. Mirror the install(DIRECTORY) logic here.
  get_target_property(KATANA_SUPPORT_SOURCE_DIR katana_support SOURCE_DIR)
  get_target_property(KATANA_SUPPORT_BINARY_DIR katana_support BINARY_DIR)
  install(
    DIRECTORY "${KATANA_SUPPORT_SOURCE_DIR}/include/" "${KATANA_SUPPORT_BINARY_DIR}/include/"
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
  get_target_property(KATANA_SOURCE_DIR katana_galois SOURCE_DIR)
  get_target_property(KATANA_BINARY_DIR katana_galois BINARY_DIR)
  install(
    DIRECTORY "${KATANA_SOURCE_DIR}/include/" "${KATANA_BINARY_DIR}/include/"
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
  )
endif()

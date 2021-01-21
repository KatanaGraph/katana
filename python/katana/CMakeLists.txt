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

set(KATANA_PYTHON_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

function(add_cython_module NAME)
  set(options)
  set(options1)
  set(optionsmany DEPENDS LIBRARIES SOURCES)
  cmake_parse_arguments(_args "${options}" "${options1}" "${optionsmany}" ${ARGN})

  add_cython_target(${NAME} ${_args_SOURCES} CXX OUTPUT_VAR _${NAME}_SOURCES)
  add_library(${NAME} MODULE ${_${NAME}_SOURCES})
  python_extension_module(${NAME})
  if (_args_DEPENDS)
    add_dependencies(${NAME} ${_args_DEPENDS})
  endif()
  if (_args_LIBRARIES)
    target_link_libraries(${NAME} ${_args_LIBRARIES})
  endif()
  file(RELATIVE_PATH current_relative_path ${KATANA_PYTHON_ROOT} ${CMAKE_CURRENT_SOURCE_DIR})
  install(
      TARGETS ${NAME}
      LIBRARY DESTINATION python/katana/${current_relative_path}
  )
endfunction()

add_cython_module(galois SOURCES galois.pyx LIBRARIES Katana::galois)
add_cython_module(numpy_structs SOURCES numpy_structs.pyx)

generate_cython_source(graphs DEPENDS graphs.instantiations.jinja)
add_cython_module(graphs
    SOURCES ${CMAKE_CURRENT_BINARY_DIR}/graphs.pyx
    DEPENDS galois graphs_pxd
    LIBRARIES Katana::galois)

generate_cython_source(atomic DEPENDS numba_wrapper_support.pyx.jinja)
add_cython_module(atomic
    SOURCES ${CMAKE_CURRENT_BINARY_DIR}/atomic.pyx
    DEPENDS galois
    LIBRARIES Katana::galois)

generate_cython_source(datastructures DEPENDS numba_wrapper_support.pyx.jinja)
add_cython_module(datastructures
    SOURCES ${CMAKE_CURRENT_BINARY_DIR}/datastructures.pyx
    DEPENDS galois numpy_structs
    LIBRARIES Katana::galois)

generate_cython_source(property_graph DEPENDS numba_wrapper_support.pyx.jinja)
add_cython_module(property_graph
    SOURCES ${CMAKE_CURRENT_BINARY_DIR}/property_graph.pyx
    DEPENDS graphs property_graph_pxd
    LIBRARIES Katana::galois)

generate_cython_source(_loops DEPENDS numba_wrapper_support.pyx.jinja)
add_cython_module(_loops
    SOURCES ${CMAKE_CURRENT_BINARY_DIR}/_loops.pyx
    DEPENDS  galois numpy_structs datastructures_pxd property_graph_pxd
    LIBRARIES Katana::galois)

add_cython_module(timer
    SOURCES timer.pyx
    LIBRARIES Katana::galois)

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

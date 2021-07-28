cmake_minimum_required(VERSION 3.17)

project(Katana)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")

include(KatanaBuildCommon)

###### Documentation ######

set_property(GLOBAL APPEND PROPERTY KATANA_DOXYGEN_DIRECTORIES
        ${CMAKE_CURRENT_SOURCE_DIR}/docs/doxygen
        ${CMAKE_CURRENT_SOURCE_DIR}/libgalois
        ${CMAKE_CURRENT_SOURCE_DIR}/libsupport
        ${CMAKE_CURRENT_SOURCE_DIR}/libtsuba)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  add_katana_doxygen_target()
endif()

add_sanitize_options()
add_sanitize_blacklist(config/sanitizers/ubsan_blacklist.txt.in)

if(KATANA_USE_SANITIZER)
  add_subdirectory(libsanitize)
endif()

add_subdirectory(libsupport)
add_subdirectory(libtsuba)
add_subdirectory(libgalois)

if(KATANA_IS_MAIN_PROJECT AND BUILD_TESTING)
  add_subdirectory(lonestar)
  # If we are testing and there is no custom input location, add rules to
  # download inputs
  if(KATANA_ENABLE_INPUTS)
    add_subdirectory(inputs)
  endif()
else()
  # If we are not testing, only build lonestar targets if they are required by
  # targets outside of lonestar.
  add_subdirectory(lonestar EXCLUDE_FROM_ALL)
endif()

add_subdirectory(tools)


####### Language Bindings #######

if(KATANA_LANG_BINDINGS_PYTHON)
  add_python_setuptools_target(katana_python
                               DEPENDS Katana::galois
                               COMPONENT python)
  if(KATANA_IS_MAIN_PROJECT AND BUILD_TESTING)
    add_python_setuptools_tests(katana_python PATH python/test)
  endif()
  add_katana_sphinx_target(katana_python)
endif()

###### Packaging ######

set(CPACK_PACKAGE_NAME "katana")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/COPYRIGHT")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")

if(NOT BUILD_SHARED_LIBS)
  set(PACKAGE_SUFFIX "${PACKAGE_SUFFIX}-static")
endif()

string(TOLOWER "${CMAKE_BUILD_TYPE}" build_type)

if(build_type STREQUAL "debug")
  set(PACKAGE_SUFFIX "${PACKAGE_SUFFIX}-dbg")
endif()

katana_setup_cpack_component_groups("${CPACK_PACKAGE_NAME}" "${PACKAGE_SUFFIX}")

set(CPACK_PACKAGE_DESCRIPTION "Katana Graph system (Open-source)")

macro(katana_cpack_open_components)
  cpack_add_component(dev GROUP dev_pkg DEPENDS shlib)
  cpack_add_component(lib GROUP dev_pkg DEPENDS shlib)

  cpack_add_component(shlib GROUP shlib_pkg)

  cpack_add_component(tools GROUP tools_pkg DEPENDS shlib)

  cpack_add_component(python GROUP python_pkg DEPENDS shlib)
endmacro()

# Use a macro here so it can be called in the enterprise build to setup components there (they are directory local)
katana_cpack_open_components()

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  katana_reformat_cpack_dependencies()
  include(CPack)
endif()

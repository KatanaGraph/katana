cmake_minimum_required(VERSION 3.13)

project(Katana)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")

include(KatanaBuildCommon)

###### Documentation ######

set_property(GLOBAL APPEND PROPERTY KATANA_DOXYGEN_DIRECTORIES
        ${CMAKE_CURRENT_SOURCE_DIR}/docs
        ${CMAKE_CURRENT_SOURCE_DIR}/libgalois
        ${CMAKE_CURRENT_SOURCE_DIR}/libsupport
        ${CMAKE_CURRENT_SOURCE_DIR}/libtsuba)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  add_katana_doxygen_target()
endif()

add_sanitize_options()
add_sanitize_blacklist(config/sanitizers/ubsan_blacklist.txt.in)

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

###### Distribution ######

set(CPACK_GENERATOR "TGZ")
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/COPYRIGHT")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
set(CPACK_PACKAGE_VERSION_MAJOR ${KATANA_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${KATANA_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${KATANA_VERSION_PATCH})
set(CPACK_PACKAGE_VERSION ${KATANA_VERSION})
include(CPack)

if(KATANA_LANG_BINDINGS_PYTHON)
  add_python_setuptools_target(katana_python
                               DEPENDS Katana::galois)
  if(KATANA_IS_MAIN_PROJECT AND BUILD_TESTING)
    add_python_setuptools_tests(katana_python PATH python/tests)
  endif()
  if(BUILD_DOCS)
    add_python_setuptools_docs(katana_python)
  endif()
endif()

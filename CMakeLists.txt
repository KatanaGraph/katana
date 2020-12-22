cmake_minimum_required(VERSION 3.13)

project(Galois)

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

add_subdirectory(libsupport)
add_subdirectory(libtsuba)
add_subdirectory(libgalois)

string(FIND "${CMAKE_PROJECT_NAME}" "${PROJECT_NAME}" project_name_position)
if(project_name_position EQUAL 0 AND BUILD_TESTING)
  add_subdirectory(lonestar)
  # If we are testing and there is no custom input location, add rules to
  # download inputs
  if(GALOIS_ENABLE_INPUTS)
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
set(CPACK_PACKAGE_VERSION_MAJOR ${GALOIS_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${GALOIS_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${GALOIS_VERSION_PATCH})
include(CPack)

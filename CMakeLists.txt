cmake_minimum_required(VERSION 3.17)

project(Katana)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")

include(BuildCommon)
include(AddUnitTest)

if("gpu" IN_LIST KATANA_COMPONENTS)
  if("${CMAKE_CUDA_ARCHITECTURES}" STREQUAL "")
    set(CMAKE_CUDA_ARCHITECTURES "52;60;70;80")
  endif()
endif()
string(COMPARE NOTEQUAL "${CMAKE_CUDA_ARCHITECTURES}" "" KATANA_USE_GPU)

###### Documentation ######

set_property(GLOBAL APPEND PROPERTY KATANA_DOXYGEN_DIRECTORIES
        ${CMAKE_CURRENT_SOURCE_DIR}/docs/doxygen
        ${CMAKE_CURRENT_SOURCE_DIR}/libgalois
        ${CMAKE_CURRENT_SOURCE_DIR}/libgraph
        ${CMAKE_CURRENT_SOURCE_DIR}/libsupport
        ${CMAKE_CURRENT_SOURCE_DIR}/libtsuba)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  add_katana_doxygen_target()
endif()

if(KATANA_USE_SANITIZER)
  determine_libasan_path()
  add_subdirectory(libsanitize)
endif()

add_sanitize_options()
add_sanitize_blacklist(config/sanitizer/ubsan_blacklist.txt.in)

if(KATANA_USE_GPU)
  enable_language(CUDA)
  find_package(CUDAToolkit)

  if(KATANA_USE_SANITIZER)
    set(CMAKE_CUDA_LINK_EXECUTABLE "<CMAKE_LINKER> <LINK_FLAGS> <OBJECTS> -o <TARGET> <LINK_LIBRARIES>")
  endif()
endif()

find_package(Flatbuffers REQUIRED)

# TODO (rashid): Replace below `find_program` calls for flatc
# with find_program(... REQUIRED) when cmake version is bumped to >=3.18
message(CHECK_START "Looking for flatc")
find_program(FLATC flatc)
if(FLATC)
  message(CHECK_PASS "found ${FLATC}")
else()
  message(FATAL_ERROR "Could NOT find flatc")
endif()

add_subdirectory(libsupport)
add_subdirectory(libtsuba)
add_subdirectory(libgalois)
add_subdirectory(libgraph)

if(KATANA_IS_MAIN_PROJECT AND cpp IN_LIST KATANA_LANG_TESTING)
  add_subdirectory(lonestar)
else()
  # If we are not testing, only build lonestar targets if they are required by
  # targets outside of lonestar.
  add_subdirectory(lonestar EXCLUDE_FROM_ALL)
endif()

add_subdirectory(tools)

add_subdirectory(libgpu)

####### Language Bindings #######


if (python IN_LIST KATANA_LANG_BINDINGS)
  find_package(pybind11 REQUIRED)
  add_subdirectory(libkatana_python_native)
  add_python_setuptools_target(katana_python
                               DEPENDS Katana::support arrow::arrow Katana::galois Katana::graph Katana::python_native
                                      pybind11::headers
                               COMPONENT python)
  if(KATANA_IS_MAIN_PROJECT AND python IN_LIST KATANA_LANG_TESTING)
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


cmake_minimum_required(VERSION 3.13)

project(pygalois)

# TODO(amp): Deduplicate this with katana/CMakeLists.txt or even better merge this into that build so there is
#  only one build entry point.

file(STRINGS ../config/version.txt GALOIS_VERSION)
string(REGEX REPLACE "[ \t\n]" "" GALOIS_VERSION ${GALOIS_VERSION})
set(GALOIS_COPYRIGHT_YEAR "2020") # Also in COPYRIGHT

# Override the scikit-build Cython modules since they are bugged in a
# way that prevents the use of generated Cython code.
list(INSERT CMAKE_MODULE_PATH 0 "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ModuleOverrides")
# TODO(amp): Check on the bug and remove this hack if a fix is released.
#  https://github.com/scikit-build/scikit-build/issues/292

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")

###### Options (alternatively pass as options to cmake -DName=Value) ######
set(BUILD_LIBGALOIS ON CACHE BOOL "Build libgalois from the source tree; if set to OFF then expect libgalois to already be installed in the environment.")
set(BUILD_DOCS OFF CACHE BOOL "Build documentation.")

include(FindPythonModule)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(PythonInterp REQUIRED)
find_package(PythonExtensions REQUIRED)
find_package(Cython REQUIRED)
find_package(NumPy REQUIRED)

find_python_module(jinja2 REQUIRED)
# HACK: For some reason find_python_module doesn't fail even though REQUIRED is
#  specified (presumably a bug). So check and fail ourselves.
if (NOT PY_JINJA2)
  message(FATAL_ERROR "Jinja2 required (for Cython code generation)")
endif()

if(BUILD_LIBGALOIS)
  add_subdirectory(.. ${CMAKE_BINARY_DIR}/../cmake-galois-build EXCLUDE_FROM_ALL)
else()
  find_package(Galois REQUIRED)
endif()

add_subdirectory(galois)
add_subdirectory(docs)

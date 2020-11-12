cmake_minimum_required(VERSION 3.13)

project(Galois)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules")

include(GNUInstallDirs)
include(FetchContent)
include(GitHeadSHA)

file(STRINGS config/version.txt GALOIS_VERSION)
string(REGEX REPLACE "[ \t\n]" "" GALOIS_VERSION ${GALOIS_VERSION})
string(REGEX REPLACE "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\1" GALOIS_VERSION_MAJOR ${GALOIS_VERSION})
string(REGEX REPLACE "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\2" GALOIS_VERSION_MINOR ${GALOIS_VERSION})
string(REGEX REPLACE "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\3" GALOIS_VERSION_PATCH ${GALOIS_VERSION})
set(GALOIS_COPYRIGHT_YEAR "2018") # Also in COPYRIGHT
set(GALOIS_GIT_SHA "${GIT_HEAD_SHA}")

if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to Release")
  # cmake default flags with relwithdebinfo is -O2 -g
  # cmake default flags with release is -O3 -DNDEBUG
  set(CMAKE_BUILD_TYPE "Release")
endif()

###### Options (alternatively pass as options to cmake -DName=Value) ######
###### General features ######
set(GALOIS_ENABLE_PAPI OFF CACHE BOOL "Use PAPI counters for profiling")
set(GALOIS_ENABLE_VTUNE OFF CACHE BOOL "Use VTune for profiling")
set(GALOIS_STRICT_CONFIG OFF CACHE BOOL "Instead of falling back gracefully, fail")
set(GALOIS_GRAPH_LOCATION "" CACHE PATH "Location of inputs for tests if downloaded/stored separately.")
set(CXX_CLANG_TIDY "" CACHE STRING "Semi-colon list specifying clang-tidy command and arguments")
set(CMAKE_CXX_COMPILER_LAUNCHER "" CACHE STRING "Semi-colon list specifying command to wrap compiler invocations (e.g., ccache)")
set(USE_ARCH native CACHE STRING "Optimize for a specific processor architecture ('none' to disable)")
set(GALOIS_USE_SANITIZER "" CACHE STRING "Semi-colon list of sanitizers to use (Memory, MemoryWithOrigins, Address, Undefined, Thread)")
# This option is automatically handled by CMake.  # It makes add_library build a shared lib unless STATIC is explicitly specified.
# Putting this here is mostly just a placeholder so people know it's an option.
# Currently this is really only intended to change anything for the libgalois_shmem target.
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries")
# This option is added by include(CTest). We define it here to let people know
# that this is a standard option.
set(BUILD_TESTING ON CACHE BOOL "Build tests")

###### Developer features ######
set(GALOIS_NUM_TEST_THREADS "" CACHE STRING "Maximum number of threads to use when running tests (default: number of physical cores)")
# GALOIS_FORCE_NON_STATIC is a transitional flag intended to turn symbol export
# errors into linker errors while the codebase transitions to hidden visibility
# by default.
set(GALOIS_FORCE_NON_STATIC OFF CACHE BOOL "Allow libraries intended to be used statically to be built as shared if BUILD_SHARED_LIBS=ON")
mark_as_advanced(GALOIS_FORCE_NON_STATIC)

if(NOT GALOIS_NUM_TEST_THREADS)
  cmake_host_system_information(RESULT GALOIS_NUM_TEST_THREADS QUERY NUMBER_OF_PHYSICAL_CORES)
endif()
if(GALOIS_NUM_TEST_THREADS LESS_EQUAL 0)
  set(GALOIS_NUM_TEST_THREADS 1)
endif()

if(NOT GALOIS_NUM_TEST_GPUS)
  if(GALOIS_ENABLE_GPU)
    set(GALOIS_NUM_TEST_GPUS 1)
  else()
    set(GALOIS_NUM_TEST_GPUS 0)
  endif()
endif()

###### Configure (users don't need to go beyond here) ######

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  include(CTest)
endif()

###### Configure compiler ######

# generate compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) #...without compiler extensions like gnu++11
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Hidden symbols break MacOS
if (NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_CXX_VISIBILITY_PRESET hidden)
  set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
endif()

# Always include debug info
add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-g>")

# GCC
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7)
    message(FATAL_ERROR "gcc must be version 7 or higher. Found ${CMAKE_CXX_COMPILER_VERSION}.")
  endif()

  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wall;-Wextra>")

  if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 9)
    # Avoid warnings from openmpi
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wno-cast-function-type>")
    # Avoid warnings from boost::counting_iterator (1.71.0)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wno-deprecated-copy>")
  endif()

  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Werror>")
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7)
    message(FATAL_ERROR "clang must be version 7 or higher. Found ${CMAKE_CXX_COMPILER_VERSION}.")
  endif()

  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wall;-Wextra>")

  if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 10)
    # Avoid warnings from boost::counting_iterator (1.71.0)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wno-deprecated-copy>")
  endif()

  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Werror>")
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wall;-Wextra>")

  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 12)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Werror>")
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 19.0.1)
    message(FATAL_ERROR "icpc must be 19.0.1 or higher. Found ${CMAKE_CXX_COMPILER_VERSION}.")
  endif()

  # Avoid warnings when using noinline for methods defined inside class defintion.
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-wd2196>")
endif()

# Enable architecture-specific optimizations
include(CheckArchFlags)
if(ARCH_FLAGS_FOUND)
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:${ARCH_CXX_FLAGS}>")
  add_compile_options("$<$<COMPILE_LANGUAGE:C>:${ARCH_C_FLAGS}>")
  add_link_options(${ARCH_LINK_FLAGS})
endif()

if(CXX_CLANG_TIDY)
  set(CMAKE_CXX_CLANG_TIDY ${CXX_CLANG_TIDY} "-header-filter=.*${PROJECT_SOURCE_DIR}.*")
  # Ignore warning flags intended for the CXX program. This only works because
  # the two compilers we care about, clang and gcc, both understand
  # -Wno-unknown-warning-option.
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wno-unknown-warning-option>")
endif()

###### Configure features ######

if(GALOIS_ENABLE_VTUNE)
  find_package(VTune REQUIRED PATHS /opt/intel/vtune_amplifier)
  include_directories(${VTune_INCLUDE_DIRS})
  add_definitions(-DGALOIS_ENABLE_VTUNE)
endif()

if(GALOIS_ENABLE_PAPI)
  find_package(PAPI REQUIRED)
  include_directories(${PAPI_INCLUDE_DIRS})
  add_definitions(-DGALOIS_ENABLE_PAPI)
endif()

find_package(NUMA)

find_package(Threads REQUIRED)

include(CheckMmap)

include(CheckHugePages)
if(NOT HAVE_HUGEPAGES AND GALOIS_STRICT_CONFIG)
  message(FATAL_ERROR "Need huge pages")
endif()

find_package(Boost 1.58.0 REQUIRED COMPONENTS filesystem serialization iostreams)

find_package(mongoc-1.0 1.6)
if(NOT mongoc-1.0_FOUND)
  message(STATUS "Library mongoc not found, not building MongoDB support")
endif()

find_package(MySQL 8.0)
if(NOT MySQL_FOUND)
  message(STATUS "Library MySQL not found, not building MySQL support")
endif()

foreach(llvm_step RANGE 2)
  # Range is [0, end]. Start version search from highest compatible version
  # first.
  math(EXPR llvm_ver "11 - ${llvm_step}")
  find_package(LLVM ${llvm_ver} QUIET CONFIG)
  if(LLVM_FOUND)
    break()
  endif()
endforeach()
if(LLVM_FOUND)
  message(STATUS "Library llvm found ${LLVM_DIR}")
else()
  message(FATAL_ERROR "Searched for LLVM 7 through 11 but did not find any compatible version")
endif()
if(NOT DEFINED LLVM_ENABLE_RTTI)
  message(FATAL_ERROR "Could not determine if LLVM has RTTI enabled.")
endif()
if(NOT ${LLVM_ENABLE_RTTI})
  message(FATAL_ERROR "Galois requires a build of LLVM that includes RTTI."
    "Most package managers do this already, but if you built LLVM"
    "from source you need to configure it with `-DLLVM_ENABLE_RTTI=ON`")
endif()
target_include_directories(LLVMSupport INTERFACE ${LLVM_INCLUDE_DIRS})

include(HandleSanitizer)

include(CheckEndian)

# Testing-only dependencies
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
  find_package(benchmark REQUIRED)
endif()

###### Test Inputs ######

if(GALOIS_GRAPH_LOCATION)
  set(BASEINPUT "${GALOIS_GRAPH_LOCATION}")
  set(BASE_VERIFICATION "${GALOIS_GRAPH_LOCATION}")
  message(STATUS "Using graph input and verification logs location ${GALOIS_GRAPH_LOCATION}")
else()
  set(BASEINPUT "${PROJECT_BINARY_DIR}/inputs/current")
  set(BASE_VERIFICATION "${PROJECT_BINARY_DIR}/inputs/current")
endif()

###### Source finding ######

add_custom_target(lib)
add_custom_target(apps)

# Core libraries (lib)

add_subdirectory(libsupport)
add_subdirectory(libtsuba)
add_subdirectory(libgalois)


if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
  add_subdirectory(lonestar)
  # If we are testing and there is no custom input location, add rules to
  # download inputs
  if(NOT GALOIS_GRAPH_LOCATION)
    add_subdirectory(inputs)
  endif()
else()
  # If we are not testing, only build lonestar targets if they are required by
  # targets outside of lonestar.
  add_subdirectory(lonestar EXCLUDE_FROM_ALL)
endif()

add_subdirectory(tools)

###### Documentation ######

find_package(Doxygen)
if(DOXYGEN_FOUND)
  set(doc_dirs)
  list(APPEND doc_dirs
    ${CMAKE_CURRENT_SOURCE_DIR}/docs
    ${CMAKE_CURRENT_SOURCE_DIR}/libgalois
    ${CMAKE_CURRENT_SOURCE_DIR}/libsupport
    ${CMAKE_CURRENT_SOURCE_DIR}/libtsuba)
  list(JOIN doc_dirs "\" \"" DOXYFILE_SOURCE_DIR)

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in
     ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile.in @ONLY)
  if(NOT TARGET doc)
    add_custom_target(doc ${DOXYGEN_EXECUTABLE}
       ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile.in WORKING_DIRECTORY
       ${CMAKE_CURRENT_BINARY_DIR})
  endif()
endif()

###### Installation ######

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/GaloisConfigVersion.cmake
  VERSION ${GALOIS_VERSION}
  COMPATIBILITY SameMajorVersion
)
configure_package_config_file(
  cmake/GaloisConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/GaloisConfig.cmake
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Galois"
  PATH_VARS CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_BINDIR
)
install(
  FILES
  "${CMAKE_CURRENT_BINARY_DIR}/GaloisConfigVersion.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/GaloisConfig.cmake"
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindNUMA.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Galois"
  COMPONENT dev
)
install(
  EXPORT GaloisTargets
  NAMESPACE Galois::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Galois"
  COMPONENT dev
)

###### Distribution ######

set(CPACK_GENERATOR "TGZ")
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/COPYRIGHT")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
set(CPACK_PACKAGE_VERSION_MAJOR ${GALOIS_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${GALOIS_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${GALOIS_VERSION_PATCH})
include(CPack)

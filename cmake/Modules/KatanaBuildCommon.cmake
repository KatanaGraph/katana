include_guard(DIRECTORY)

include(GNUInstallDirs)
include(FetchContent)
include(GitHeadSHA)
include(KatanaPythonSetupSubdirectory)

file(STRINGS ${CMAKE_CURRENT_LIST_DIR}/../../config/version.txt KATANA_VERSION)
string(REGEX REPLACE "[ \t\n]" "" KATANA_VERSION ${KATANA_VERSION})
string(REGEX REPLACE "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\1" KATANA_VERSION_MAJOR ${KATANA_VERSION})
string(REGEX REPLACE "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\2" KATANA_VERSION_MINOR ${KATANA_VERSION})
string(REGEX REPLACE "([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\3" KATANA_VERSION_PATCH ${KATANA_VERSION})
set(KATANA_COPYRIGHT_YEAR "2018") # Also in COPYRIGHT
set(KATANA_GIT_SHA "${GIT_HEAD_SHA}")

if (NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to Release")
  # cmake default flags with relwithdebinfo is -O2 -g
  # cmake default flags with release is -O3 -DNDEBUG
  set(CMAKE_BUILD_TYPE "Release")
endif ()

###### Options (alternatively pass as options to cmake -DName=Value) ######
###### General features ######
set(KATANA_ENABLE_PAPI OFF CACHE BOOL "Use PAPI counters for profiling")
set(KATANA_ENABLE_VTUNE OFF CACHE BOOL "Use VTune for profiling")
set(KATANA_STRICT_CONFIG OFF CACHE BOOL "Instead of falling back gracefully, fail")
set(KATANA_GRAPH_LOCATION "" CACHE PATH "Location of inputs for tests if downloaded/stored separately.")
set(CXX_CLANG_TIDY "" CACHE STRING "Semi-colon separated list of clang-tidy command and arguments")
set(CMAKE_CXX_COMPILER_LAUNCHER "" CACHE STRING "Semi-colon separated list of command and arguments to wrap compiler invocations (e.g., ccache)")
set(KATANA_USE_ARCH "sandybridge" CACHE STRING "Semi-colon separated list of processor architectures to attempt to optimize for; use the first valid configuration ('none' to disable)")
set(KATANA_USE_SANITIZER "" CACHE STRING "Semi-colon separated list of sanitizers to use (Memory, MemoryWithOrigins, Address, Undefined, Thread)")
# This option is automatically handled by CMake.
# It makes add_library build a shared lib unless STATIC is explicitly specified.
# Putting this here is mostly just a placeholder so people know it's an option.
# Currently this is really only intended to change anything for the libkatana_galois target.
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries")
# This option is added by include(CTest). We define it here to let people know
# that this is a standard option.
set(BUILD_TESTING ON CACHE BOOL "Build tests")
# Set here to override the cmake default of "/usr/local" because
# "/usr/local/lib" is not a default search location for ld.so
set(CMAKE_INSTALL_PREFIX "/usr" CACHE STRING "install prefix")

set(KATANA_LANG_BINDINGS "" CACHE STRING "Semi-colon separated list of language bindings to build (e.g., 'python'). Default: none")

###### Developer features ######
set(KATANA_PER_ROUND_STATS OFF CACHE BOOL "Report statistics of each round of execution")
set(KATANA_NUM_TEST_GPUS "" CACHE STRING "Number of test GPUs to use (on a single machine) for running the tests.")
set(KATANA_USE_LCI OFF CACHE BOOL "Use LCI network runtime instead of MPI")
set(KATANA_NUM_TEST_THREADS "" CACHE STRING "Maximum number of threads to use when running tests (default: number of physical core)")
set(KATANA_AUTO_CONAN OFF CACHE BOOL "Automatically call conan from cmake rather than manually (experimental)")
# KATANA_FORCE_NON_STATIC is a transitional flag intended to turn symbol export
# errors into linker errors while the codebase transitions to hidden visibility
# by default.
set(KATANA_FORCE_NON_STATIC OFF CACHE BOOL "Allow libraries intended to be used statically to be built as shared if BUILD_SHARED_LIBS=ON")
mark_as_advanced(KATANA_FORCE_NON_STATIC)

cmake_host_system_information(RESULT KATANA_NUM_PHYSICAL_CORES QUERY NUMBER_OF_PHYSICAL_CORES)

if (NOT KATANA_NUM_TEST_THREADS)
  set(KATANA_NUM_TEST_THREADS ${KATANA_NUM_PHYSICAL_CORES})
endif ()
if (KATANA_NUM_TEST_THREADS LESS_EQUAL 0)
  set(KATANA_NUM_TEST_THREADS 1)
endif ()

if (NOT KATANA_NUM_TEST_GPUS)
  if (KATANA_ENABLE_GPU)
    set(KATANA_NUM_TEST_GPUS 1)
  else ()
    set(KATANA_NUM_TEST_GPUS 0)
  endif ()
endif ()

###### Configure (users don't need to go beyond here) ######

# Enable KATANA_IS_MAIN_PROJECT if this file is included in the root project.
# KATANA_IS_MAIN_PROJECT is enabled for Katana library builds and disabled if
# Katana is included as a sub-project of another build.
if (CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  set(KATANA_IS_MAIN_PROJECT ON)
else ()
  set(KATANA_IS_MAIN_PROJECT OFF)
endif ()

if (KATANA_IS_MAIN_PROJECT)
  include(CTest)
endif ()

###### Install dependencies ######

find_package(PkgConfig)

if (KATANA_AUTO_CONAN)
  include(${CMAKE_CURRENT_LIST_DIR}/conan.cmake)
  # config/conanfile.py is relative to the current project, so it will be either enterprise or open depending on who
  # includes us.
  conan_cmake_run(CONANFILE config/conanfile.py
      BASIC_SETUP
      CMAKE_TARGETS
      NO_OUTPUT_DIRS
      BUILD missing)
  include("${CMAKE_CURRENT_BINARY_DIR}/conan_paths.cmake")
endif ()


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
endif ()

# Always include debug info
add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-g>")

# GCC
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7)
    message(FATAL_ERROR "gcc must be version 7 or higher. Found ${CMAKE_CXX_COMPILER_VERSION}.")
  endif ()

  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wall;-Wextra>")

  if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 9)
    # Avoid warnings from openmpi
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wno-cast-function-type>")
    # Avoid warnings from boost::counting_iterator (1.71.0)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wno-deprecated-copy>")
  endif ()

  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Werror>")
  endif ()
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7)
    message(FATAL_ERROR "clang must be version 7 or higher. Found ${CMAKE_CXX_COMPILER_VERSION}.")
  endif ()

  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wall;-Wextra>")

  if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 10)
    # Avoid warnings from boost::counting_iterator (1.71.0)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wno-deprecated-copy>")
  endif ()

  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Werror>")
  endif ()
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wall;-Wextra>")

  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 12)
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Werror>")
  endif ()
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 19.0.1)
    message(FATAL_ERROR "icpc must be 19.0.1 or higher. Found ${CMAKE_CXX_COMPILER_VERSION}.")
  endif ()

  # Avoid warnings when using noinline for methods defined inside class defintion.
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-wd2196>")
endif ()

# Enable architecture-specific optimizations
include(CheckArchFlags)
if (ARCH_FLAGS_FOUND)
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:${ARCH_CXX_FLAGS}>")
  add_compile_options("$<$<COMPILE_LANGUAGE:C>:${ARCH_C_FLAGS}>")
  add_link_options(${ARCH_LINK_FLAGS})
endif ()

if (CXX_CLANG_TIDY)
  set(CMAKE_CXX_CLANG_TIDY ${CXX_CLANG_TIDY} "-header-filter=.*${PROJECT_SOURCE_DIR}.*")
  # Ignore warning flags intended for the CXX program. This only works because
  # the two compilers we care about, clang and gcc, both understand
  # -Wno-unknown-warning-option.
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-Wno-unknown-warning-option>")
endif ()

# Setup for bindings

message(STATUS "Building Katana language bindings: ${KATANA_LANG_BINDINGS}")

if(python IN_LIST KATANA_LANG_BINDINGS)
  include(FindPythonModule)
  find_package (Python3 COMPONENTS Interpreter Development NumPy)

  find_python_module(setuptools REQUIRED)
  find_python_module(Cython REQUIRED)
  find_python_module(numpy REQUIRED)
  find_python_module(sphinx)

  if(NOT BUILD_SHARED_LIBS)
    message(ERROR "Cannot build Python binding without BUILD_SHARED_LIBS")
  endif()

  set(KATANA_LANG_BINDINGS_PYTHON TRUE)
endif()


###### Configure features ######

if (KATANA_ENABLE_VTUNE)
  find_package(VTune REQUIRED PATHS /opt/intel/vtune_amplifier)
  include_directories(${VTune_INCLUDE_DIRS})
  add_definitions(-DKATANA_ENABLE_VTUNE)
endif ()

if (KATANA_ENABLE_PAPI)
  find_package(PAPI REQUIRED)
  include_directories(${PAPI_INCLUDE_DIRS})
  add_definitions(-DKATANA_ENABLE_PAPI)
endif ()

find_package(NUMA)

find_package(Threads REQUIRED)

include(CheckMmap)

include(CheckHugePages)
if (NOT HAVE_HUGEPAGES AND KATANA_STRICT_CONFIG)
  message(FATAL_ERROR "Need huge pages")
endif ()

find_package(Boost 1.58.0 REQUIRED COMPONENTS filesystem serialization iostreams)

find_package(mongoc-1.0 1.6)
if (NOT mongoc-1.0_FOUND)
  message(STATUS "Library mongoc not found, not building MongoDB support")
endif ()

find_package(MySQL 8.0)
if (NOT MySQL_FOUND)
  message(STATUS "Library MySQL not found, not building MySQL support")
endif ()

# Search for the highest compatible version of LLVM from the list of versions here.
# LLVM minor versions are not ABI compatible, so we need to specifically say we support the 11.1 special release.
message(CHECK_START "Looking for LLVM")
foreach (llvm_ver IN ITEMS 11.1 11.0 10 9 8 7)
  find_package(LLVM ${llvm_ver} QUIET CONFIG)
  if (LLVM_FOUND)
    break()
  endif ()
endforeach ()
if (LLVM_FOUND)
  message(CHECK_PASS "found version ${LLVM_VERSION} (${LLVM_DIR})")
else ()
  message(FATAL_ERROR "Searched for LLVM 9 through 11.1 but did not find any compatible version")
endif ()
if (NOT DEFINED LLVM_ENABLE_RTTI)
  message(FATAL_ERROR "Could not determine if LLVM has RTTI enabled.")
endif ()
if (NOT ${LLVM_ENABLE_RTTI})
  message(FATAL_ERROR "Galois requires a build of LLVM that includes RTTI."
      "Most package managers do this already, but if you built LLVM"
      "from source you need to configure it with `-DLLVM_ENABLE_RTTI=ON`")
endif ()
target_include_directories(LLVMSupport INTERFACE ${LLVM_INCLUDE_DIRS})

include(HandleSanitizer)

include(CheckEndian)

# Testing-only dependencies
if (CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
  find_package(benchmark REQUIRED)
endif ()

###### Common Functions ######

function(set_common_katana_library_options)
  set(no_value_options ALWAYS_SHARED)
  set(one_value_options)
  set(multi_value_options)

  cmake_parse_arguments(X "${no_value_options}" "${one_value_options}" "${multi_value_options}" ${ARGN})

  set(target ${X_UNPARSED_ARGUMENTS})
  # Some careful defines and build directives to ensure things work on Windows
  # (see config.h):
  #
  # 1. When we build our shared library, KATANA_EXPORT => dllexport
  # 2. When someone uses our shared library, KATANA_EXPORT => dllimport
  # 3. When we build a static library, KATANA_EXPORT => ""
  #
  # In the world of ELF, 1 and 2 can both be handled with visibility("default")
  if (BUILD_SHARED_LIBS OR X_ALWAYS_SHARED)
    target_compile_definitions(${target} PRIVATE KATANA_SHARED_LIB_BUILDING)
  else ()
    target_compile_definitions(${target} PRIVATE KATANA_STATIC_LIB)
  endif()

  # Having a single definition of a vtable has two benefits:
  #
  # 1. Reduces the size of intermediate object files
  # 2. More importantly, gives canonical typeinfo pointers so that derived objects in
  #    different libraries can resolve to the same base class typeinfo.
  if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:CXX>:-Wweak-vtables>")
  endif ()
endfunction ()

###### Test Inputs ######

if (KATANA_GRAPH_LOCATION)
  set(BASEINPUT "${KATANA_GRAPH_LOCATION}")
  set(BASE_VERIFICATION "${KATANA_GRAPH_LOCATION}")
  set(KATANA_ENABLE_INPUTS OFF)
  message(STATUS "Using graph input and verification logs location ${KATANA_GRAPH_LOCATION}")
else ()
  set(BASEINPUT "${PROJECT_BINARY_DIR}/inputs/current")
  set(BASE_VERIFICATION "${PROJECT_BINARY_DIR}/inputs/current")
  set(KATANA_ENABLE_INPUTS ON)
endif ()
# Set a common graph location for any nested projects.
set(KATANA_GRAPH_LOCATION ${BASEINPUT})

###### Documentation ######

find_package(Doxygen)

###### Source finding ######

add_custom_target(lib)
add_custom_target(apps)
add_custom_target(tools)

# Core libraries (lib)

# Allow build tree libraries and executables to see preload customizations like
# in libtsuba-fs without having to set LD_PRELOAD or similar explicitly.
list(PREPEND CMAKE_BUILD_RPATH ${PROJECT_BINARY_DIR})

# Allow installed libraries and executables to pull in deployment specific
# modifications like vendored runtime libraries (e.g., MPI).
list(PREPEND CMAKE_INSTALL_RPATH ${CMAKE_INSTALL_PREFIX}/katana/lib)

###### Installation ######

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/KatanaConfigVersion.cmake
    VERSION ${KATANA_VERSION}
    COMPATIBILITY SameMajorVersion
)
configure_package_config_file(
    ${CMAKE_CURRENT_LIST_DIR}/../KatanaConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/KatanaConfig.cmake
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Katana"
    PATH_VARS CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_BINDIR
)
install(
    FILES
    "${CMAKE_CURRENT_BINARY_DIR}/KatanaConfigVersion.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/KatanaConfig.cmake"
    "${CMAKE_CURRENT_LIST_DIR}/FindNUMA.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Katana"
    COMPONENT dev
)
install(
    EXPORT KatanaTargets
    NAMESPACE Katana::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Katana"
    COMPONENT dev
)

set_property(GLOBAL PROPERTY KATANA_DOXYGEN_DIRECTORIES)
# Invoke this after all the documentation directories have been added to KATANA_DOXYGEN_DIRECTORIES.
function(add_katana_doxygen_target)
  if (NOT TARGET doc AND DOXYGEN_FOUND)
    get_property(doc_dirs GLOBAL PROPERTY KATANA_DOXYGEN_DIRECTORIES)
    list(JOIN doc_dirs "\" \"" DOXYFILE_SOURCE_DIR)

    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(doc
                      # Delete the old html docs before rebuilding, because doxygen will find it's own build files
                      # during search and get confused if the build directory is a subdirectory of the source directory.
                      # amp looked several times and found no way to limit the doxygen search path involved.
                      COMMAND rm -rf ${CMAKE_CURRENT_BINARY_DIR}/html
                      COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
                      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                      BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/html
                      DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
  endif ()
endfunction()

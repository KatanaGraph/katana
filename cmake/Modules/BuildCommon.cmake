include_guard(DIRECTORY)

include(GNUInstallDirs)
include(FetchContent)
include(GitHeadSHA)
include(PythonSetupSubdirectory)

include(Version)
include(FindCLibrary)

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
set(KATANA_USE_PAPI OFF CACHE BOOL "Use PAPI counters for profiling")
set(KATANA_USE_VTUNE OFF CACHE BOOL "Use VTune for profiling")
set(KATANA_STRICT_CONFIG OFF CACHE BOOL "Instead of falling back gracefully, fail")
set(KATANA_GRAPH_LOCATION "" CACHE PATH "Location of inputs for tests if downloaded/stored separately.")
set(KATANA_USE_COVERAGE OFF CACHE BOOL "Add instrumentation (used for code coverage collection) to binaries.")
set(CXX_CLANG_TIDY "" CACHE STRING "Semi-colon separated list of clang-tidy command and arguments")
set(CMAKE_CXX_COMPILER_LAUNCHER "" CACHE STRING "Semi-colon separated list of command and arguments to wrap compiler invocations (e.g., ccache)")
set(KATANA_USE_ARCH "sandybridge" CACHE STRING "Semi-colon separated list of processor architectures to use features of;
  Any older/incompatible processors will be unable to run resulting binaries.
  Use the first valid configuration ('none' to disable). Default: 'sandybridge'")
set(KATANA_USE_TUNE "intel;generic;auto" CACHE STRING "Semi-colon separated list of processor architectures to attempt to optimize for.
  Use the first valid configuration (the 'auto' is replaced with KATANA_USE_ARCH, 'none' to disable).
  Default: 'intel;generic;auto' which tries to optimize for the most recent Intel processors, then falls back to
  optimizing for the most common processors and then to optimizing for the processor selected by KATANA_USE_ARCH")
set(KATANA_USE_SANITIZER "" CACHE STRING "Semi-colon separated list of sanitizers to use (Memory, MemoryWithOrigins, Address, Undefined, Thread)")
set(KATANA_USE_JEMALLOC OFF CACHE BOOL "Use jemalloc")

# This option is automatically handled by CMake.
# It makes add_library build a shared lib unless STATIC is explicitly specified.
set(BUILD_SHARED_LIBS YES CACHE BOOL "Build shared libraries. Default: YES")
# This option is added by include(CTest). We define it here to let people know
# that this is a standard option.
set(BUILD_TESTING ON CACHE BOOL "Build tests")
set(BUILD_DOCS OFF CACHE BOOL "Build documentation with make doc")
set(BUILD_EXTERNAL_DOCS OFF CACHE BOOL "Hide '*-draft*' documentation pages and directories when building documentation")
# Set here to override the cmake default of "/usr/local" because
# "/usr/local/lib" is not a default search location for ld.so
set(CMAKE_INSTALL_PREFIX "/usr" CACHE STRING "install prefix")

set(KATANA_LANG_BINDINGS "" CACHE STRING "Semi-colon separated list of language bindings to build (e.g., 'python'). Default: none")

set(KATANA_COMPONENTS "" CACHE STRING "Semi-colon separated list of optional components to build")

###### Packaging features ######
set(KATANA_PACKAGE_TYPE "deb" CACHE STRING "Semi-colon separated list of package types to build with cpack. Supported values: deb.")
set(KATANA_PACKAGE_DIRECTORY "${PROJECT_BINARY_DIR}/pkg" CACHE STRING "The output path for packages.")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "" CACHE STRING "Semi-colon separated list of Debian package dependencies")
#set(CPACK_RPM_PACKAGE_DEPENDS "" CACHE STRING "Semi-colon separated list of RPM package dependencies")

###### Developer features ######
set(KATANA_PER_ROUND_STATS OFF CACHE BOOL "Report statistics of each round of execution")
set(KATANA_NUM_TEST_GPUS "" CACHE STRING "Number of test GPUs to use (on a single machine) for running the tests.")
set(KATANA_USE_LCI OFF CACHE BOOL "Use LCI network runtime instead of MPI")
set(KATANA_NUM_TEST_THREADS "" CACHE STRING "Maximum number of threads to use when running tests (default: min(number of physical core, 4))")
set(KATANA_AUTO_CONAN OFF CACHE BOOL "Automatically call conan from cmake rather than manually (experimental)")

###### Configure (users don't need to go beyond here) ######

# Without these, build tree shared libraries are not used on a machine where Katana is already installed
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--disable-new-dtags")
SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--disable-new-dtags")
SET(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Wl,--disable-new-dtags")

cmake_host_system_information(RESULT KATANA_NUM_PHYSICAL_CORES QUERY NUMBER_OF_PHYSICAL_CORES)

if (NOT KATANA_NUM_TEST_THREADS)
  set(KATANA_NUM_TEST_THREADS ${KATANA_NUM_PHYSICAL_CORES})
  if (KATANA_NUM_TEST_THREADS GREATER_EQUAL 4)
    set(KATANA_NUM_TEST_THREADS 4)
  endif ()
endif ()
if (KATANA_NUM_TEST_THREADS LESS_EQUAL 0)
  set(KATANA_NUM_TEST_THREADS 1)
endif ()

if (NOT KATANA_NUM_TEST_GPUS)
  if (KATANA_USE_GPU)
    set(KATANA_NUM_TEST_GPUS 1)
  else ()
    set(KATANA_NUM_TEST_GPUS 0)
  endif ()
endif ()

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

set(supported_components)
if (KATANA_SUPPORTED_COMPONENTS)
  set(supported_components "${KATANA_SUPPORTED_COMPONENTS}")
endif ()

foreach (comp ${KATANA_COMPONENTS})
  if (NOT "${comp}" IN_LIST supported_components)
    message(FATAL_ERROR "Invalid value for KATANA_COMPONENTS: ${comp}. Accepted values: ${supported_components}")
  endif ()
endforeach ()

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

if (python IN_LIST KATANA_LANG_BINDINGS)
  include(FindPythonModule)
  find_package (Python3 COMPONENTS Interpreter Development NumPy)

  find_python_module(setuptools REQUIRED)
  find_python_module(Cython REQUIRED)
  find_python_module(numpy REQUIRED)
  if (BUILD_DOCS)
    find_python_module(sphinx REQUIRED)
  endif ()

  if (NOT BUILD_SHARED_LIBS)
    message(FATAL_ERROR "Cannot build Python binding without BUILD_SHARED_LIBS")
  endif ()

  set(KATANA_LANG_BINDINGS_PYTHON TRUE)
endif ()

###### Configure features ######

if (KATANA_USE_VTUNE)
  find_package(VTune REQUIRED PATHS /opt/intel/vtune_amplifier)
  include_directories(${VTune_INCLUDE_DIRS})
  add_definitions(-DKATANA_USE_VTUNE)
endif ()

if (KATANA_USE_PAPI)
  find_package(PAPI REQUIRED)
  include_directories(${PAPI_INCLUDE_DIRS})
  add_definitions(-DKATANA_USE_PAPI)
endif ()

if (KATANA_USE_JEMALLOC)
  find_c_library(NAME jemalloc TARGET jemalloc::jemalloc REQUIRED MAIN_HEADER jemalloc/jemalloc.h)
  link_libraries(jemalloc::jemalloc)
endif ()

find_package(NUMA)

find_package(Threads REQUIRED)

include(CheckMmap)

include(CheckHugePages)
if (KATANA_STRICT_CONFIG)
  if (NOT HAVE_HUGEPAGES)
    message(FATAL_ERROR "Strict config requires huge pages but huge pages not found")
  endif ()
  if (KATANA_USE_JEMALLOC)
    message(FATAL_ERROR "Strict config requires huge pages but jemalloc disables huge pages")
  endif ()
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

message(CHECK_START "Looking for LLVM")
# Honor the build environment search order for LLVM versions. This may result in using a slightly older version of
# llvm-support in cases where multiple are installed, however it enables the environment to select an older versions
# intentionally if required. Conda does this and not allowing it to do so causes link errors.
find_package(LLVM CONFIG)

if(NOT LLVM_FOUND OR LLVM_VERSION VERSION_LESS 7)
  message(FATAL_ERROR "Could not find LLVM. LLVM (>=7) is required. May have found a version: ${LLVM_VERSION} (${LLVM_DIR})")
else()
  message(CHECK_PASS "found version ${LLVM_VERSION} (${LLVM_DIR})")
endif()

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

find_package(Arrow REQUIRED)
if(TARGET arrow::arrow)
  # Conan package
  # These are the names we use elsewhere.
else()
  # Libarrow project cmake
  get_filename_component(ARROW_CONFIG_DIR ${Arrow_CONFIG} DIRECTORY)
  find_package(Parquet REQUIRED HINTS ${ARROW_CONFIG_DIR})
  add_library(arrow::arrow ALIAS arrow_shared)
  add_library(arrow::parquet ALIAS parquet_shared)
endif()

# Testing-only dependencies
if (CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
  find_package(benchmark REQUIRED)
endif ()

# Instrument binaries if desired
if (KATANA_USE_COVERAGE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g --coverage")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g --coverage")
  add_link_options("SHELL: --coverage -lgcov")
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
  set(KATANA_USE_INPUTS OFF)
  message(STATUS "Using graph input and verification logs location ${KATANA_GRAPH_LOCATION}")
else ()
  set(BASEINPUT "${PROJECT_BINARY_DIR}/inputs/current")
  set(BASE_VERIFICATION "${PROJECT_BINARY_DIR}/inputs/current")
  set(KATANA_USE_INPUTS ON)
endif ()
# Set a common graph location for any nested projects.
set(KATANA_GRAPH_LOCATION ${BASEINPUT})

###### Documentation ######

if (BUILD_DOCS)
  find_package(Doxygen REQUIRED)
endif()

###### Source finding ######

add_custom_target(lib)
add_custom_target(apps)
add_custom_target(tools)

# Core libraries (lib)

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

# Use both "doc" and "docs" to make people with different mental conventions happy
add_custom_target(docs)
add_custom_target(doc)
add_dependencies(doc docs)

set_property(GLOBAL PROPERTY KATANA_DOXYGEN_DIRECTORIES)
# Invoke this after all the documentation directories have been added to KATANA_DOXYGEN_DIRECTORIES.
function(add_katana_doxygen_target)
  if (NOT BUILD_DOCS)
    return()
  endif ()
  get_property(doc_dirs GLOBAL PROPERTY KATANA_DOXYGEN_DIRECTORIES)
  list(JOIN doc_dirs "\" \"" DOXYFILE_SOURCE_DIR)

  file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/docs/doxygen)
  configure_file(${PROJECT_SOURCE_DIR}/docs/Doxyfile.in ${PROJECT_BINARY_DIR}/docs/Doxyfile @ONLY)
  add_custom_target(doxygen_docs
      COMMAND ${CMAKE_COMMAND} -E rm -rf ${PROJECT_BINARY_DIR}/docs/doxygen
      COMMAND ${DOXYGEN_EXECUTABLE} ${PROJECT_BINARY_DIR}/docs/Doxyfile
      WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
      DEPENDS ${PROJECT_BINARY_DIR}/docs/Doxyfile
      COMMENT "Building Doxygen docs from ${PROJECT_BINARY_DIR}/docs/Doxyfile")
endfunction()

add_custom_target(sphinx_docs)
add_dependencies(docs sphinx_docs)

function(add_katana_sphinx_target target_name)
  if (NOT BUILD_DOCS)
    return()
  endif ()

  get_target_property(PYTHON_ENV_SCRIPT ${target_name} PYTHON_ENV_SCRIPT)
  if (NOT BUILD_EXTERNAL_DOCS)
    set(sphinx_build_flags "-W -t internal")
    add_custom_target(
      ${target_name}_sphinx_docs
      COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/docs/${target_name}
      COMMAND env KATANA_DOXYGEN_PATH="${CMAKE_BINARY_DIR}/docs/doxygen" ${PYTHON_ENV_SCRIPT} sphinx-build
        -W
        -b html
	-t internal
        ${PROJECT_SOURCE_DIR}/docs
        ${CMAKE_BINARY_DIR}/docs/${target_name}
      COMMAND ${CMAKE_COMMAND} -E echo "${target_name} documentation at file://${CMAKE_BINARY_DIR}/docs/${target_name}/index.html"
      COMMENT "Building internal ${target_name} sphinx documentation"
    )
  else ()
    add_custom_target(
      ${target_name}_sphinx_docs
      COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/docs/${target_name}
      COMMAND env KATANA_DOXYGEN_PATH="${CMAKE_BINARY_DIR}/docs/doxygen" ${PYTHON_ENV_SCRIPT} sphinx-build
        -b html
	-t external
        ${PROJECT_SOURCE_DIR}/docs
        ${CMAKE_BINARY_DIR}/docs/${target_name}
      COMMAND ${CMAKE_COMMAND} -E echo "${target_name} documentation at file://${CMAKE_BINARY_DIR}/docs/${target_name}/index.html"
      COMMENT "Building external ${target_name} sphinx documentation"
    )
  endif ()


  # The root of documentation is sphinx_docs, which include doxygen_docs via
  # the breathe extension
  add_dependencies(${target_name}_sphinx_docs ${target_name} doxygen_docs)
  add_dependencies(sphinx_docs ${target_name}_sphinx_docs)
endfunction()

###### Packaging ######

include(CPackComponent)

set(CPACK_PACKAGE_DIRECTORY "${KATANA_PACKAGE_DIRECTORY}")
string(TOUPPER "${KATANA_PACKAGE_TYPE}" CPACK_GENERATOR)
set(CPACK_PACKAGE_VENDOR "Katana Graph")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://katanagraph.com")
set(CPACK_PACKAGE_VERSION_MAJOR ${KATANA_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${KATANA_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${KATANA_VERSION_PATCH})
# The debian version sorts correctly for RPMs too
set(CPACK_PACKAGE_VERSION ${KATANA_VERSION_DEBIAN})

set(CPACK_PACKAGE_CONTACT "support@katanagraph.com")

# Debian package specific options
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_ENABLE_COMPONENT_DEPENDS TRUE)
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS TRUE)
set(CPACK_DEB_COMPONENT_INSTALL TRUE)

#list(APPEND CPACK_DEBIAN_PACKAGE_DEPENDS)

set(tmp "${CPACK_GENERATOR}")
list(REMOVE_ITEM tmp "DEB")
if(tmp)
  message(WARNING "The requested package types, ${tmp}, are not supported and will almost certainly not be correct. You are on your own from here.")
endif()

# We don't support RPMs, but since these meta data was already written, include it for the future.
# RPM specific options
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)
set(CPACK_RPM_PACKAGE_AUTOREQPROV FALSE)
set(CPACK_RPM_COMPONENT_INSTALL TRUE)

# Commented out RPM dependencies. Better to have obviously no dependencies that subtly wrong ones. This will need updating if it is used again.
#list(APPEND CPACK_RPM_PACKAGE_DEPENDS libatomic numactl-libs ncurses-libs)

# Create a package for each component group.
set(CPACK_COMPONENTS_GROUPING ONE_PER_GROUP)

# Setup the cpack component groups: dev_pkg, shlib_pkg, tools_pkg.
# After calling this, call cpack_add_component to add components to the groups.
macro(katana_setup_cpack_component_groups NAME SUFFIX)
  # The groups are named *_pkg to distinguish them from the components themselves.

  set(CPACK_DEBIAN_DEV_PKG_PACKAGE_NAME "lib${NAME}-dev${SUFFIX}")
  set(CPACK_RPM_DEV_PKG_PACKAGE_NAME "${NAME}-dev${SUFFIX}")
  # No addition dependencies on dev_pkg since it depends on shlib_pkg
  cpack_add_component_group(dev_pkg)
  set(CPACK_COMPONENT_DEV_PKG_DESCRIPTION "Katana Graph development libraries and headers")
  set(CPACK_COMPONENT_DEV_PKG_DEPENDS shlib_pkg)

  set(CPACK_DEBIAN_SHLIB_PKG_PACKAGE_NAME "lib${NAME}${SUFFIX}")
  list(APPEND CPACK_DEBIAN_SHLIB_PKG_PACKAGE_DEPENDS ${CPACK_DEBIAN_PACKAGE_DEPENDS})
  set(CPACK_RPM_SHLIB_PKG_PACKAGE_NAME "${NAME}${SUFFIX}")
  list(APPEND CPACK_RPM_SHLIB_PKG_PACKAGE_DEPENDS ${CPACK_RPM_PACKAGE_DEPENDS})
  cpack_add_component_group(shlib_pkg)
  set(CPACK_COMPONENT_SHLIB_PKG_DESCRIPTION "Katana Graph runtime libraries")

  set(CPACK_DEBIAN_TOOLS_PKG_PACKAGE_NAME "${NAME}-tools${SUFFIX}")
  set(CPACK_RPM_TOOLS_PKG_PACKAGE_NAME "${NAME}-tools${SUFFIX}")
  # No addition dependencies on tools_pkg since it depends on shlib_pkg
  cpack_add_component_group(tools_pkg)
  set(CPACK_COMPONENT_TOOLS_PKG_DESCRIPTION "Katana Graph system management and data processing tools")
  set(CPACK_COMPONENT_TOOLS_PKG_DEPENDS shlib_pkg)

  set(CPACK_DEBIAN_PYTHON_PKG_PACKAGE_NAME "python3-${NAME}${SUFFIX}")
  list(APPEND CPACK_DEBIAN_PYTHON_PKG_PACKAGE_DEPENDS "python3-minimal" "python3-numpy")
  set(CPACK_RPM_PYTHON_PKG_PACKAGE_NAME "python-${NAME}${SUFFIX}")
#  To add RPM python support we will need something like: list(APPEND CPACK_RPM_PYTHON_PACKAGE_DEPENDS "[the name of the python3 rpm]")
  # No addition dependencies on apps_pkg since it depends on shlib_pkg
  cpack_add_component_group(python_pkg)
  set(CPACK_COMPONENT_PYTHON_PKG_DESCRIPTION "Katana Graph Python API")
  set(CPACK_COMPONENT_PYTHON_PKG_DEPENDS shlib_pkg)
endmacro()

# Convert all `CPACK_.*_PACKAGE_DEPENDS` variables from cmake lists (;-separated) to valid dependency lists (,-separated).
macro(katana_reformat_cpack_dependencies)
  get_cmake_property(_variables VARIABLES)
  foreach(var IN LISTS _variables)
    string(REGEX MATCH "CPACK_.*_PACKAGE_DEPENDS" _match "${var}")
    if (_match)
      list(JOIN "${var}" ", " _out)
      set("${var}" "${_out}")
    endif()
  endforeach()
endmacro()

# Dump all cpack variables. Useful for debugging.
macro(katana_dump_cpack_config)
  get_cmake_property(_variables VARIABLES)
  foreach(var IN LISTS _variables)
    string(REGEX MATCH "CPACK_.*" _match "${var}")
    if (_match)
      message("set(${var} \"${${var}}\")")
    endif()
  endforeach()
endmacro()

# Build support for plugins

set(KATANA_BUILD_PLUGINS_DIR ${PROJECT_BINARY_DIR}/plugins)
set(KATANA_INSTALL_PLUGINS_DIR ${CMAKE_INSTALL_LIBDIR}/katana/plugins)

file(MAKE_DIRECTORY ${KATANA_BUILD_PLUGINS_DIR})
file(WRITE ${KATANA_BUILD_PLUGINS_DIR}/.placeholder
     "This directory contains dynamically loaded plugins for the Katana system.")
install(
    FILES ${KATANA_BUILD_PLUGINS_DIR}/.placeholder
    DESTINATION "${KATANA_INSTALL_PLUGINS_DIR}"
    COMPONENT shlib
)

# Install a katana plugin.
#
# This does not install any headers associated with the target. This is a plugin
# it should generally not have headers. However, a separate install call for
# headers will work.
function(install_katana_plugin)
  set(no_value_options INSTALL_AS_LIBRARY)
  set(one_value_options TARGET COMPONENT)
  set(multi_value_options )

  cmake_parse_arguments(X "${no_value_options}" "${one_value_options}" "${multi_value_options}" ${ARGN})

  # Put a link to the plugin in the build plugin directory
  add_custom_command(
      TARGET ${X_TARGET}
      POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:${X_TARGET}>
        ${KATANA_BUILD_PLUGINS_DIR}/$<TARGET_FILE_NAME:${X_TARGET}>
  )

  set_target_properties(${X_TARGET} PROPERTIES ADDITIONAL_CLEAN_FILES
                        ${KATANA_BUILD_PLUGINS_DIR}/$<TARGET_FILE_NAME:${X_TARGET}>)

  if (X_INSTALL_AS_LIBRARY)
    # TODO(amp): This is deprecated! It makes plugins visitable as libraries. If more than one plugin is used as a
    # library in the same application it WILL fail to link.

    # Install the plugin as a normal library.
    install(
        TARGETS ${X_TARGET}
        LIBRARY
          DESTINATION "${CMAKE_INSTALL_LIBDIR}"
          COMPONENT ${X_COMPONENT}
    )
    # Create a link from the plugins directory to the installed library.
    install(CODE "
file(MAKE_DIRECTORY \$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${KATANA_INSTALL_PLUGINS_DIR})
file(CREATE_LINK
  ../../$<TARGET_FILE_NAME:${X_TARGET}>
  \$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${KATANA_INSTALL_PLUGINS_DIR}/$<TARGET_FILE_NAME:${X_TARGET}>
  SYMBOLIC)"
      COMPONENT ${X_COMPONENT}
    )
  else()
    # Install the plugin only as a plugin and not as a library.
    install(
        TARGETS ${X_TARGET}
        EXPORT ${X_EXPORT}
        LIBRARY
        DESTINATION "${KATANA_INSTALL_PLUGINS_DIR}"
        COMPONENT ${X_COMPONENT}
    )
  endif()
endfunction()

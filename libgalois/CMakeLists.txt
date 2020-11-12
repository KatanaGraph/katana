add_library(galois_shmem)
add_library(Galois::shmem ALIAS galois_shmem)
set_target_properties(galois_shmem PROPERTIES EXPORT_NAME shmem)
add_dependencies(lib galois_shmem)

configure_file(src/Version.cpp.in Version.cpp @ONLY)

set(sources
        "${CMAKE_CURRENT_BINARY_DIR}/Version.cpp"
        src/Barrier_Counting.cpp
        src/Barrier.cpp
        src/Barrier_Dissemination.cpp
        src/Barrier_MCS.cpp
        src/Barrier_Simple.cpp
        src/Barrier_Topo.cpp
        src/Context.cpp
        src/Deterministic.cpp
        src/DynamicBitset.cpp
        src/FileGraph.cpp
        src/FileGraphParallel.cpp
        src/gIO.cpp
        src/GraphHelpers.cpp
        src/HWTopo.cpp
        src/Mem.cpp
        src/NumaMem.cpp
        src/OCFileGraph.cpp
        src/OutIndexView.cpp
        src/PageAlloc.cpp
        src/PagePool.cpp
        src/ParaMeter.cpp
        src/PerThreadStorage.cpp
        src/Profile.cpp
        src/PropertyFileGraph.cpp
        src/PtrLock.cpp
        src/SharedMem.cpp
        src/SharedMemSys.cpp
        src/SimpleLock.cpp
        src/Statistics.cpp
        src/Support.cpp
        src/Termination.cpp
        src/ThreadPool.cpp
        src/Threads.cpp
        src/ThreadTimer.cpp
        src/Timer.cpp
)

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  list(APPEND sources src/HWTopoDarwin.cpp)
else()
  include(CheckSchedSetAffinity)
  if (NOT SCHED_SETAFFINITY_FOUND)
    if (GALOIS_STRICT_CONFIG)
      message(FATAL_ERROR "Need sched_setaffinity")
    endif()
  endif()
  list(APPEND sources src/HWTopoLinux.cpp)
endif()

target_sources(galois_shmem PRIVATE ${sources})

target_include_directories(galois_shmem PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

target_link_libraries(galois_shmem PUBLIC tsuba)
target_link_libraries(galois_shmem PRIVATE Threads::Threads)
target_link_libraries(galois_shmem PUBLIC galois_support)

# Some careful defines and build directives to ensure things work on Windows
# (see config.h):
#
# 1. When we build our shared library, GALOIS_EXPORT => dllexport
# 2. When someone uses our shared library, GALOIS_EXPORT => dllimport
# 3. When we build a static library, GALOIS_EXPORT => ""
#
# In the world of ELF, 1 and 2 can both be handled with visibility("default")
if(BUILD_SHARED_LIBS)
  target_compile_definitions(galois_shmem PRIVATE GALOIS_SHARED_LIB_BUILDING)
else()
  target_compile_definitions(galois_shmem PRIVATE GALOIS_STATIC_LIB)
endif()

if(SCHED_SETAFFINITY_FOUND)
  target_compile_definitions(galois_shmem PRIVATE GALOIS_USE_SCHED_SETAFFINITY)
  target_link_libraries(galois_shmem PRIVATE ${SCHED_SETAFFINITY_LIBRARIES})
endif()

if(NUMA_FOUND)
  target_compile_definitions(galois_shmem PRIVATE GALOIS_USE_NUMA)
  target_link_libraries(galois_shmem PRIVATE ${NUMA_LIBRARY})
else()
  message(WARNING "No NUMA Support.  Likely poor performance for multi-socket systems.")
endif()

if(VTune_FOUND)
  target_link_libraries(galois_shmem PRIVATE ${VTune_LIBRARIES})
endif()

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
  add_subdirectory(test)
endif()

install(
  DIRECTORY include/
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT dev
  FILES_MATCHING PATTERN "*.h"
)

install(
  TARGETS galois_shmem
  EXPORT GaloisTargets
  LIBRARY
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT shlib
  ARCHIVE
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT lib
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

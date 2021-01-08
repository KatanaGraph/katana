add_library(katana_galois)
add_library(Katana::galois ALIAS katana_galois)
set_target_properties(katana_galois PROPERTIES EXPORT_NAME galois)
add_dependencies(lib katana_galois)

configure_file(src/Version.cpp.in Version.cpp @ONLY)

set(sources
        "${CMAKE_CURRENT_BINARY_DIR}/Version.cpp"
        src/Barrier_Counting.cpp
        src/Barrier.cpp
        src/Barrier_Dissemination.cpp
        src/Barrier_MCS.cpp
        src/Barrier_Simple.cpp
        src/Barrier_Topo.cpp
        src/BuildGraph.cpp
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
        src/OpLog.cpp
        src/PageAlloc.cpp
        src/PagePool.cpp
        src/ParaMeter.cpp
        src/PerThreadStorage.cpp
        src/Profile.cpp
        src/PropertyFileGraph.cpp
        src/PropertyViews.cpp
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
        src/analytics/Utils.cpp
        src/analytics/bfs/bfs.cpp
        src/analytics/sssp/sssp.cpp
        src/analytics/connected_components/connected_components.cpp
        src/analytics/jaccard/jaccard.cpp
)

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  list(APPEND sources src/HWTopoDarwin.cpp)
else()
  include(CheckSchedSetAffinity)
  if (NOT SCHED_SETAFFINITY_FOUND)
    if (KATANA_STRICT_CONFIG)
      message(FATAL_ERROR "Need sched_setaffinity")
    endif()
  endif()
  list(APPEND sources src/HWTopoLinux.cpp)
endif()

target_sources(katana_galois PRIVATE ${sources})

target_include_directories(katana_galois PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

target_link_libraries(katana_galois PUBLIC tsuba)
target_link_libraries(katana_galois PRIVATE Threads::Threads)
target_link_libraries(katana_galois PUBLIC katana_support)

# Some careful defines and build directives to ensure things work on Windows
# (see config.h):
#
# 1. When we build our shared library, KATANA_EXPORT => dllexport
# 2. When someone uses our shared library, KATANA_EXPORT => dllimport
# 3. When we build a static library, KATANA_EXPORT => ""
#
# In the world of ELF, 1 and 2 can both be handled with visibility("default")
if(BUILD_SHARED_LIBS)
  target_compile_definitions(katana_galois PRIVATE KATANA_SHARED_LIB_BUILDING)
else()
  target_compile_definitions(katana_galois PRIVATE KATANA_STATIC_LIB)
endif()

if(SCHED_SETAFFINITY_FOUND)
  target_compile_definitions(katana_galois PRIVATE KATANA_USE_SCHED_SETAFFINITY)
  target_link_libraries(katana_galois PRIVATE ${SCHED_SETAFFINITY_LIBRARIES})
endif()

if(NUMA_FOUND)
  target_compile_definitions(katana_galois PRIVATE KATANA_USE_NUMA)
  target_link_libraries(katana_galois PRIVATE ${NUMA_LIBRARY})
else()
  message(WARNING "No NUMA Support.  Likely poor performance for multi-socket systems.")
endif()

if(VTune_FOUND)
  target_link_libraries(katana_galois PRIVATE ${VTune_LIBRARIES})
endif()

if(KATANA_IS_MAIN_PROJECT AND BUILD_TESTING)
  add_subdirectory(test)
endif()

install(
  DIRECTORY include/
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT dev
  FILES_MATCHING PATTERN "*.h"
)

install(
  TARGETS katana_galois
  EXPORT KatanaTargets
  LIBRARY
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT shlib
  ARCHIVE
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT lib
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

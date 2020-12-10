# Find architecture-specific flags based on the value of GALOIS_USE_ARCH.
# GALOIS_USE_ARCH is a cmake list of possible architecture names. Return the
# flags corresponding to the first architecture available for the current
# compiler toolchain.
#
# Once done this will define
#  ARCH_FLAGS_FOUND
#  ARCH_CXX_FLAGS - Compiler flags to enable architecture-specific optimizations
#  ARCH_C_FLAGS - Compiler flags to enable architecture-specific optimizations
#  ARCH_LINK_FLAGS - Compiler flags to enable architecture-specific optimizations
include(CheckCXXCompilerFlag)

if(ARCH_FLAGS_FOUND)
  return()
endif()

if(NOT GALOIS_USE_ARCH OR GALOIS_USE_ARCH STREQUAL "none" OR ARCH_FLAGS_FOUND)
  set(ARCH_CXX_FLAGS_CANDIDATES)
else()
  foreach(FLAG ${GALOIS_USE_ARCH})
    if(FLAG STREQUAL "mic")
      if(CMAKE_CXX_COMPILER_ID MATCHES "Intel")
        list(APPEND ARCH_CXX_FLAGS_CANDIDATES -mmic)
      endif()

      if(CMAKE_COMPILER_IS_GNUCC)
        list(APPEND ARCH_CXX_FLAGS_CANDIDATES -march=knc)
      endif()
    else()
      list(APPEND ARCH_CXX_FLAGS_CANDIDATES "-march=${FLAG}")
    endif()
  endforeach()
endif()

foreach(FLAG ${ARCH_CXX_FLAGS_CANDIDATES})
  check_cxx_compiler_flag("${FLAG}" ARCH_CXX_MARCH_FLAG_DETECTED)
  if(ARCH_CXX_MARCH_FLAG_DETECTED)
    set(ARCH_FLAGS_FOUND "YES")
    set(ARCH_CXX_FLAGS "${FLAG}")
    set(ARCH_C_FLAGS "${FLAG}")
    set(ARCH_LINK_FLAGS "${FLAG}")
    break()
  endif()
endforeach()
unset(ARCH_CXX_MARCH_FLAG_DETECTED)

if(ARCH_FLAGS_FOUND)
  foreach(FLAG "-mtune=generic")
    check_cxx_compiler_flag("${FLAG}" ARCH_CXX_MTUNE_FLAG_DETECTED)
    if(ARCH_CXX_MTUNE_FLAG_DETECTED)
      list(APPEND ARCH_CXX_FLAGS "${FLAG}")
      list(APPEND ARCH_C_FLAGS "${FLAG}")
      list(APPEND ARCH_LINK_FLAGS "${FLAG}")
      break()
    endif()
  endforeach()
endif()
unset(ARCH_CXX_MTUNE_FLAG_DETECTED)

message(STATUS "Using architecture flags: ${ARCH_CXX_FLAGS}")

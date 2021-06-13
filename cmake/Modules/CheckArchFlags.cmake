# Find architecture-specific flags based on the values of KATANA_USE_ARCH and
# KATANA_USE_TUNE. KATANA_USE_ARCH and KATANA_USE_TUNE are cmake lists of
# possible architecture names. Return the flags corresponding to the first
# architecture available for the current compiler toolchain for both arch
# (-march) and tune (-mtune).
#
# For maximum performance, but no compatibility with different computers, use:
# -DKATANA_USE_ARCH=native -DKATANA_USE_TUNE=native
#
# The default is:
# -DKATANA_USE_ARCH=sandybridge -DKATANA_USE_TUNE=intel;generic;auto
# which tries to optimize for the most recent Intel processors, then falls back
# to optimizing for the most common processors and then to optimizing for
# sandybridge.
#
# Once done this will define
#  ARCH_FLAGS_FOUND
#  ARCH_CXX_FLAGS - Compiler flags to enable architecture-specific optimizations
#  ARCH_C_FLAGS - Compiler flags to enable architecture-specific optimizations
#  ARCH_LINK_FLAGS - Compiler flags to enable architecture-specific optimizations
include(CheckCXXCompilerFlag)

# TODO(amp): This code duplicates the same process for arch and tune. Dedup.
# TODO(amp): If KATANA_USE_ARCH or KATANA_USE_TUNE contain " " or ";", this will fail.

# Determine and add -march
if(NOT KATANA_USE_ARCH OR KATANA_USE_ARCH STREQUAL "none" OR ARCH_FLAGS_FOUND)
  set(ARCH_CXX_FLAGS_CANDIDATES)
else()
  foreach(FLAG ${KATANA_USE_ARCH})
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

set(CMAKE_REQUIRED_QUIET TRUE)
foreach(FLAG ${ARCH_CXX_FLAGS_CANDIDATES})
  message(CHECK_START "Trying architecture argument ${FLAG}")
  check_cxx_compiler_flag("${FLAG}" "ARCH_CXX_MARCH_FLAG_DETECTED-${FLAG}")
  if(ARCH_CXX_MARCH_FLAG_DETECTED-${FLAG})
    message(CHECK_PASS "supported")
    set(ARCH_FLAGS_FOUND "YES")
    set(ARCH_CXX_FLAGS "${FLAG}")
    set(ARCH_C_FLAGS "${FLAG}")
    set(ARCH_LINK_FLAGS "${FLAG}")
    break()
  else()
    message(CHECK_FAIL "unsupported")
  endif()
endforeach()
unset(CMAKE_REQUIRED_QUIET)

# Determine and add -mtune
if(NOT KATANA_USE_TUNE OR KATANA_USE_TUNE STREQUAL "none" OR TUNE_FLAGS_FOUND)
  set(TUNE_CXX_FLAGS_CANDIDATES)
else()
  list(TRANSFORM KATANA_USE_TUNE REPLACE "^auto$" "${KATANA_USE_ARCH}")
  foreach(FLAG ${KATANA_USE_TUNE})
    if(FLAG STREQUAL "mic")
      if(CMAKE_CXX_COMPILER_ID MATCHES "Intel")
        # TODO(amp): Does icc support an mtune like flag for "mic"?
        # list(APPEND TUNE_CXX_FLAGS_CANDIDATES -mmic)
      endif()
      if(CMAKE_COMPILER_IS_GNUCC)
        list(APPEND TUNE_CXX_FLAGS_CANDIDATES -mtune=knc)
      endif()
    else()
      list(APPEND TUNE_CXX_FLAGS_CANDIDATES "-mtune=${FLAG}")
    endif()
  endforeach()
endif()

set(CMAKE_REQUIRED_QUIET TRUE)
foreach(FLAG ${TUNE_CXX_FLAGS_CANDIDATES})
  message(CHECK_START "Trying tune argument ${FLAG}")
  check_cxx_compiler_flag("${ARCH_CXX_FLAGS} ${FLAG}" "ARCH_CXX_MTUNE_FLAG_DETECTED-${FLAG}")
  if(ARCH_CXX_MTUNE_FLAG_DETECTED-${FLAG})
    message(CHECK_PASS "supported")
    set(TUNE_FLAGS_FOUND "YES")
    list(APPEND ARCH_CXX_FLAGS "${FLAG}")
    list(APPEND ARCH_C_FLAGS "${FLAG}")
    list(APPEND ARCH_LINK_FLAGS "${FLAG}")
    break()
  else()
    message(CHECK_FAIL "unsupported")
  endif()
endforeach()
unset(CMAKE_REQUIRED_QUIET)

list(JOIN ARCH_CXX_FLAGS " " ARCH_CXX_FLAGS_STR)
message(STATUS "Using architecture flags: ${ARCH_CXX_FLAGS_STR}")
unset(ARCH_CXX_FLAGS_STR)

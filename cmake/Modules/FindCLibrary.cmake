# find_c_library finds a named library and populates a CMake target with the
# appropriate build options for use with target_link_libraries.
#
# It tries the following methods, in order:
#
# 1. find_package(<name>): Look for a cmake module for <name>
# 2. pkg_check_modules(<name>): Look for a pkg-config config for <name>
# 3. find_library(<name>): Look for lib<name> and MAIN_HEADER in path
#
# The exact paths searched by find_c_library varies based on the method and
# version of CMake.
#
# If find_c_library fails to find a library, possible ways to adjust the search
# path of find_c_library, find_package, pkg_check_modules, etc. are
#
# 1. Add the directory the environment variable PATH
# 2. Add the directory to the CMake variable CMAKE_PREFIX_PATH
# 3. Add the directory to the CMake variable ${LIBRARY_NAME}_ROOT
function(find_c_library)
  set(options REQUIRED)
  set(one_value_args TARGET NAME MAIN_HEADER)
  set(multi_value_args)
  cmake_parse_arguments(X "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  if(TARGET ${X_NAME})
    return()
  endif()

  message(CHECK_START "Looking for ${X_NAME}")
  find_package(${X_NAME} QUIET)
  if(TARGET ${X_TARGET})
    # Target-based CMake package
    get_target_property(loc ${X_TARGET} INTERFACE_LINK_LIBRARIES)
    message(CHECK_PASS "found (via cmake target): ${loc}")
    return()
  endif()
  if(${X_NAME}_FOUND)
    # Non target-based approach
    add_library(${X_TARGET} INTERFACE IMPORTED)
    set_target_properties(${X_TARGET} PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${${X_NAME}_INCLUDE_DIRS}"
      IMPORTED_LOCATION "${${X_NAME}_LIBRARIES}"
      INTERFACE_LINK_LIBRARIES "${${X_NAME}_LIBRARIES}")
    get_target_property(loc ${X_TARGET} INTERFACE_LINK_LIBRARIES)
    message(CHECK_PASS "found (via cmake): ${loc}")
    return()
  endif()

  pkg_check_modules(${X_NAME} IMPORTED_TARGET GLOBAL ${X_NAME} QUIET)
  if(${X_NAME}_FOUND)
    add_library(${X_TARGET} ALIAS PkgConfig::${X_NAME})
    get_target_property(loc ${X_TARGET} INTERFACE_LINK_LIBRARIES)
    message(CHECK_PASS "found (via pkg-config): ${loc}")
    return()
  endif()

  set(include_dir)
  if(X_MAIN_HEADER)
    # Make find_path and find_library behave more similarly to their behavior
    # when called from find_package by adding ${lib}_ROOT to the search path
    # list.
    find_path(include_dir ${X_MAIN_HEADER} HINTS ${${X_NAME}_ROOT})
    if(NOT include_dir)
      message(CHECK_FAIL "not found")
      if(X_REQUIRED)
        message(FATAL_ERROR "${X_NAME} not found")
      endif()
      return()
    endif()
  endif()

  find_library(library ${X_NAME} HINTS ${${X_NAME}_ROOT})
  if(NOT library)
    message(CHECK_FAIL "not found")
    if(X_REQUIRED)
      message(FATAL_ERROR "${X_NAME} not found")
    endif()
    return()
  endif()

  add_library(${X_TARGET} INTERFACE IMPORTED)
  set_target_properties(${X_TARGET} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${include_dir}"
    IMPORTED_LOCATION "${library}"
    INTERFACE_LINK_LIBRARIES "${library}")

  # Set cache variable just in case and for human checking
  set(${X_NAME}_FOUND TRUE CACHE INTERNAL "")
  set(${X_NAME}_INCLUDE_DIRS ${include_dir} CACHE INTERNAL "")
  set(${X_NAME}_LIBRARIES ${library} CACHE INTERNAL "")

  message(CHECK_PASS "found (via find_library): ${library}; ${include_dir}")
endfunction()


# Find libcypher-parser
# defines
# CypherParser_FOUND
# CypherParser_INCLUDE_DIRS
# CypherParser_LIBRARIES
# and the cypherparser interface library to use as a target

if(TARGET cypherparser)
  return()
endif()

# find include path and library
find_path(CypherParser_INCLUDE_DIR cypher-parser.h PATHS /usr/include )
if (CypherParser_INCLUDE_DIR STREQUAL "CypherParser_INCLUDE_DIR-NOTFOUND")
  message(FATAL_ERROR "Could not find cypher-parser header.")
endif()

set(CypherParser_LIBRARY_PATH_CANDIDATES lib lib64 lib32 x86_64-linux-gnu)
find_library(CypherParser_LIBRARY cypher-parser PATHS /usr/lib/ PATH_SUFFIXES ${CypherParser_LIBRARY_PATH_CANDIDATES})

# determine if library is found
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CypherParser DEFAULT_MSG CypherParser_LIBRARY CypherParser_INCLUDE_DIR)
mark_as_advanced(CypherParser_LIBRARY CypherParser_INCLUDE_DIR)

# set the plural versions of the libraries
set(CypherParser_INCLUDE_DIRS ${CypherParser_INCLUDE_DIR})
set(CypherParser_LIBRARIES ${CypherParser_LIBRARY})

add_library(cypherparser STATIC IMPORTED)
set_target_properties(cypherparser PROPERTIES IMPORTED_LOCATION ${CypherParser_LIBRARIES})
set_target_properties(cypherparser PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CypherParser_INCLUDE_DIRS})

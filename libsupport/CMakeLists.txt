if(GALOIS_FORCE_NON_STATIC)
  add_library(galois_support)
else()
  add_library(galois_support STATIC)
endif()
add_library(Galois::support ALIAS galois_support)
set_target_properties(galois_support PROPERTIES EXPORT_NAME support)
add_dependencies(lib galois_support)

configure_file(include/galois/config.h.in include/galois/config.h)

set(sources
        src/Backtrace.cpp
        src/Env.cpp
        src/ErrorCode.cpp
        src/Http.cpp
        src/JSON.cpp
        src/Logging.cpp
        src/Random.cpp
        src/Strings.cpp
        src/Uri.cpp
)

target_sources(galois_support PRIVATE ${sources})

target_include_directories(galois_support PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

if(BUILD_SHARED_LIBS)
  target_compile_definitions(galois_support PRIVATE GALOIS_SHARED_LIB_BUILDING)
else()
  target_compile_definitions(galois_support PRIVATE GALOIS_STATIC_LIB)
endif()

find_package(nlohmann_json 3.7.3 REQUIRED)
target_link_libraries(galois_support PUBLIC nlohmann_json::nlohmann_json)

find_package(libcurl QUIET)
if(libcurl_FOUND)
  # Older conan-generated package name is libcurl. More recent versions (at
  # least 1.29) generate the cmake standard name CURL.
  if("${libcurl_VERSION}" VERSION_LESS 7.66)
    message(FATAL_ERROR "curl must be version 7.66 or higher. Found ${libcurl_VERSION}.")
  endif()
  set(curl_lib libcurl::libcurl)
else()
  find_package(CURL REQUIRED)
  set(curl_lib CURL::libcurl)
endif()
target_link_libraries(galois_support PUBLIC ${curl_lib})

# Backtrace support
find_package(Backward REQUIRED)
target_link_libraries(galois_support PUBLIC Backward::Backward)

find_package(fmt REQUIRED)
if(fmt_VERSION VERSION_LESS 4)
  message(FATAL_ERROR "fmt must be version 4 or higher. Found ${fmt_VERSION}.")
endif()
target_link_libraries(galois_support PUBLIC fmt::fmt)

if(TARGET Boost::Boost)
  target_link_libraries(galois_support PUBLIC Boost::Boost)
else()
  # Standard CMake Boost module, shorten the link time if we can
  target_link_libraries(galois_support PUBLIC Boost::boost)
  target_link_libraries(galois_support PUBLIC Boost::filesystem)
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
  DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/include/
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT dev
  FILES_MATCHING PATTERN "*.h"
)

install(
  TARGETS galois_support
  EXPORT GaloisTargets
  LIBRARY
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT shlib
  ARCHIVE
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT lib
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

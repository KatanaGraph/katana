add_library(katana_support SHARED)
add_library(Katana::support ALIAS katana_support)
set_target_properties(katana_support PROPERTIES EXPORT_NAME support)
add_dependencies(lib katana_support)

if(KATANA_USE_SANITIZER)
  # Ensure that katana_sanitize is available everywhere in the build, but don't link it to anything
  add_dependencies(katana_support katana_sanitize)
endif()

configure_file(include/katana/config.h.in include/katana/config.h)

set(sources
        src/ArrowInterchange.cpp
        src/ArrowVisitor.cpp
        src/Backtrace.cpp
        src/CommBackend.cpp
        src/Env.cpp
        src/ErrorCode.cpp
        src/HostAllocator.cpp
        src/HTTP.cpp
        src/JSON.cpp
        src/Logging.cpp
        src/Random.cpp
        src/Result.cpp
        src/Plugin.cpp
        src/Signals.cpp
        src/Strings.cpp
        src/URI.cpp
)

target_sources(katana_support PRIVATE ${sources})

target_include_directories(katana_support PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

set_common_katana_library_options(katana_support ALWAYS_SHARED)

find_package(Arrow REQUIRED)
if(TARGET arrow::arrow)
  # Conan package
  target_link_libraries(katana_support PUBLIC arrow::arrow arrow::parquet)
else()
  # Libarrow project cmake
   get_filename_component(ARROW_CONFIG_DIR ${Arrow_CONFIG} DIRECTORY)
   find_package(Parquet REQUIRED HINTS ${ARROW_CONFIG_DIR})
   target_link_libraries(katana_support PUBLIC arrow_shared parquet_shared)
endif()


find_package(nlohmann_json 3.7.3 REQUIRED)
target_link_libraries(katana_support PUBLIC nlohmann_json::nlohmann_json)

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
target_link_libraries(katana_support PUBLIC ${curl_lib})

# Backtrace support
find_package(Backward REQUIRED)
target_link_libraries(katana_support PUBLIC Backward::Backward)

find_package(fmt REQUIRED)
if(fmt_VERSION VERSION_LESS 4)
  message(FATAL_ERROR "fmt must be version 4 or higher. Found ${fmt_VERSION}.")
endif()
target_link_libraries(katana_support PUBLIC fmt::fmt)

if(TARGET Boost::Boost)
  target_link_libraries(katana_support PUBLIC Boost::Boost)
else()
  # Standard CMake Boost module, shorten the link time if we can
  target_link_libraries(katana_support PUBLIC Boost::boost)
  target_link_libraries(katana_support PUBLIC Boost::filesystem)
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
  DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/include/
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT dev
  FILES_MATCHING PATTERN "*.h"
)

install(
  TARGETS katana_support
  EXPORT KatanaTargets
  LIBRARY
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT shlib
  ARCHIVE
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT lib
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

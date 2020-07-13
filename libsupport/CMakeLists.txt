add_library(galois_support STATIC)
add_library(Galois::support ALIAS galois_support)
set_target_properties(galois_support PROPERTIES EXPORT_NAME support)
add_dependencies(lib galois_support)

set(sources
        src/GetEnv.cpp
        src/Logging.cpp
        src/FileSystem.cpp
		src/ErrorCode.cpp
)

target_sources(galois_support PRIVATE ${sources})

target_include_directories(galois_support PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

find_package(fmt REQUIRED)
if (fmt_VERSION VERSION_LESS 4)
  message(FATAL_ERROR "fmt must be version 4 or higher. Found ${fmt_VERSION}.")
endif()
target_link_libraries(galois_support PUBLIC fmt::fmt)

if (TARGET Boost::Boost)
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

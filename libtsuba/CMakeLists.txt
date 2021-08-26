add_library(tsuba SHARED)

add_dependencies(lib tsuba)

set(sources
  src/AddProperties.cpp
  src/AsyncOpGroup.cpp
  src/Errors.cpp
  src/FaultTest.cpp
  src/file.cpp
  src/FileFrame.cpp
  src/FileStorage.cpp
  src/FileView.cpp
  src/GlobalState.cpp
  src/LocalStorage.cpp
  src/ParquetReader.cpp
  src/ParquetWriter.cpp
  src/RDG.cpp
  src/RDGCore.cpp
  src/RDGHandleImpl.cpp
  src/RDGLineage.cpp
  src/RDGManifest.cpp
  src/RDGPartHeader.cpp
  src/RDGPrefix.cpp
  src/RDGSlice.cpp
  src/ReadGroup.cpp
  src/tsuba.cpp
  src/WriteGroup.cpp
)

target_sources(tsuba PRIVATE ${sources})

target_include_directories(tsuba PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
set_common_katana_library_options(tsuba ALWAYS_SHARED)

target_link_libraries(tsuba PUBLIC katana_support)

if(KATANA_IS_MAIN_PROJECT AND BUILD_TESTING)
  add_subdirectory(test)
endif()

install(
  DIRECTORY include/
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT dev
  FILES_MATCHING PATTERN "*.h"
)

install(TARGETS tsuba
  EXPORT KatanaTargets
  LIBRARY
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT shlib
  ARCHIVE
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT lib
  INCLUDES DESTINATION "${RELATIVE_INCLUDE_FROM_INSTALL_PREFIX}"
)

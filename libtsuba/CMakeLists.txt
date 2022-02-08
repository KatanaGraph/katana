add_library(katana_tsuba SHARED)
add_library(Katana::tsuba ALIAS katana_tsuba)
set_target_properties(katana_tsuba PROPERTIES EXPORT_NAME tsuba)

add_dependencies(lib katana_tsuba)

set(sources
  src/AddProperties.cpp
  src/AsyncOpGroup.cpp
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
  src/RDGTopology.cpp
  src/RDGTopologyManager.cpp
  src/PartitionTopologyMetadata.cpp
  src/ReadGroup.cpp
  src/tsuba.cpp
  src/TxnContext.cpp
  src/WriteGroup.cpp
)

target_sources(katana_tsuba PRIVATE ${sources})

target_include_directories(katana_tsuba PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
set_common_katana_library_options(katana_tsuba ALWAYS_SHARED)

target_link_libraries(katana_tsuba PUBLIC katana_support)
target_link_libraries(katana_tsuba PUBLIC katana_galois)

if(KATANA_IS_MAIN_PROJECT AND BUILD_TESTING)
  add_subdirectory(test)
endif()

install(
  DIRECTORY include/
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT dev
  FILES_MATCHING PATTERN "*.h"
)

install(TARGETS katana_tsuba
  EXPORT KatanaTargets
  LIBRARY
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT shlib
  ARCHIVE
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT lib
  INCLUDES DESTINATION "${RELATIVE_INCLUDE_FROM_INSTALL_PREFIX}"
)

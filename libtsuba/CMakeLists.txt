add_library(katana_tsuba SHARED)
add_library(Katana::tsuba ALIAS katana_tsuba)
set_target_properties(katana_tsuba PROPERTIES EXPORT_NAME tsuba)
add_dependencies(lib katana_tsuba)

include(GenerateFlatbuffers)

set(fb_files
  fbs/storage_operations.fbs
)

generate_cpp_flatbuffers(tsuba_fb_cpp "${fb_files}")

set(sources
  src/AddProperties.cpp
  src/AsyncOpGroup.cpp
  src/EntityTypeManager.cpp
  src/FaultTest.cpp
  src/file.cpp
  src/FileFrame.cpp
  src/FileStorage.cpp
  src/FileView.cpp
  src/GlobalState.cpp
  src/LocalStorage.cpp
  src/ParquetReader.cpp
  src/ParquetWriter.cpp
  src/PartitionTopologyMetadata.cpp
  src/RDG.cpp
  src/RDGCore.cpp
  src/RDGHandleImpl.cpp
  src/RDGLineage.cpp
  src/RDGManifest.cpp
  src/RDGPartHeader.cpp
  src/RDGPrefix.cpp
  src/RDGSlice.cpp
  src/RDGStorageFormatVersion.cpp
  src/RDGTopology.cpp
  src/RDGTopologyManager.cpp
  src/ReadGroup.cpp
  src/StorageBackedArrowArray.cpp
  src/StorageBackedArrowTable.cpp
  src/TxnContext.cpp
  src/WriteGroup.cpp
  src/tsuba.cpp  
  ${tsuba_fb_cpp}
)

target_sources(katana_tsuba PRIVATE ${sources})

target_include_directories(katana_tsuba PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(katana_tsuba PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
set_common_katana_library_options(katana_tsuba ALWAYS_SHARED)

target_link_libraries(katana_tsuba PUBLIC katana_support)
target_link_libraries(katana_tsuba PUBLIC katana_galois)
target_link_libraries(katana_tsuba PUBLIC flatbuffers::flatbuffers)

if(KATANA_IS_MAIN_PROJECT AND cpp IN_LIST KATANA_LANG_TESTING)
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

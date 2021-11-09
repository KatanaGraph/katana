add_executable(manifest-test manifest.cpp)
target_link_libraries(manifest-test tsuba)
target_include_directories(manifest-test PRIVATE ../src)
add_test(NAME manifest COMMAND manifest-test ${BASEINPUT}/propertygraphs/rmat15/katana_vers00000000000000000001_rdg.manifest)
set_property(TEST manifest APPEND PROPERTY LABELS quick)

add_executable(file-view-test file-view.cpp)
target_link_libraries(file-view-test tsuba)
add_test(NAME file-view COMMAND file-view-test "${CMAKE_CURRENT_BINARY_DIR}/file-view-test-wd")
set_tests_properties(file-view PROPERTIES FIXTURES_REQUIRED file-view-ready LABELS quick)
add_test(NAME clean-file-view COMMAND ${CMAKE_COMMAND} -E rm -rf "${CMAKE_CURRENT_BINARY_DIR}/file-view-test-wd")
set_tests_properties(clean-file-view PROPERTIES FIXTURES_SETUP file-view-ready LABELS quick)

add_executable(parquet-test parquet.cpp)
target_link_libraries(parquet-test tsuba)
add_test(NAME parquet COMMAND parquet-test "${CMAKE_CURRENT_BINARY_DIR}/parquet-test-wd")
set_tests_properties(parquet PROPERTIES FIXTURES_REQUIRED parquet-ready LABELS quick)
add_test(NAME clean-parquet COMMAND ${CMAKE_COMMAND} -E rm -rf "${CMAKE_CURRENT_BINARY_DIR}/parquet-test-wd")
set_tests_properties(clean-parquet PROPERTIES FIXTURES_SETUP parquet-ready LABELS quick)


## Storage Format Version backwards compatibility tests ##

## storage format version 2
## storage format version 2 is not supported

## storage format version 3
## We can't do v1-v3 tests here in libtsuba because we depends on PropertyGraph to extract EntityTypeIDs from properties
## see libgalois/test/storage-format-version*

# Test optional topologies loading/storing cycle on v3 input
add_executable(storage-format-version-v3-v3-optional-topologies-test storage-format-version/v3-optional-topologies.cpp)
target_link_libraries(storage-format-version-v3-v3-optional-topologies-test tsuba)
target_include_directories(storage-format-version-v3-v3-optional-topologies-test PRIVATE ../src)
add_test(NAME storage-format-version-v3-v3-optional-topologies COMMAND storage-format-version-v3-v3-optional-topologies-test ${BASEINPUT}/propertygraphs/ldbc_003_storage_format_version_3)
set_property(TEST storage-format-version-v3-v3-optional-topologies APPEND PROPERTY LABELS quick)
set_tests_properties(storage-format-version-v3-v3-optional-topologies PROPERTIES LABELS quick)


# Test entity type id loading/storing cycle on v3 input
add_executable(storage-format-version-v3-v3-uint16-entity-type-ids-test storage-format-version/v3-uint16-entity-type-ids.cpp)
target_link_libraries(storage-format-version-v3-v3-uint16-entity-type-ids-test tsuba)
target_include_directories(storage-format-version-v3-v3-uint16-entity-type-ids-test PRIVATE ../src)
add_test(NAME storage-format-version-v3-v3-uint16-entity-type-ids COMMAND storage-format-version-v3-v3-uint16-entity-type-ids-test ${BASEINPUT}/propertygraphs/ldbc_003_storage_format_version_3)

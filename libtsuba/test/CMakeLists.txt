set(name manifest)
set(test_name ${name}-test)
add_executable(${test_name} manifest.cpp)
target_link_libraries(${test_name} katana_tsuba)
target_include_directories(${test_name} PRIVATE ../src)
add_test(NAME ${name} COMMAND ${test_name} ${RDG_RMAT15}/katana_vers00000000000000000001_rdg.manifest)
set_property(TEST ${name} APPEND PROPERTY LABELS quick)

set(name rdg-part-header)
set(test_name ${name}-test)
add_executable(${test_name} rdg-part-header.cpp)
target_link_libraries(${test_name} katana_tsuba)
target_include_directories(${test_name} PRIVATE ../src)
add_test(NAME ${name} COMMAND ${test_name} ${RDG_RMAT15}/part_vers00000000000000000001_rdg_node00000)
set_property(TEST ${name} APPEND PROPERTY LABELS quick)

set(name rdg-slice)
set(test_name ${name}-test)
add_executable(${test_name} rdg-slice.cpp)
target_link_libraries(${test_name} katana_tsuba)
target_include_directories(${test_name} PRIVATE ../src)
add_test(NAME ${name} COMMAND ${test_name} ${RDG_LDBC_003}/katana_vers00000000000000000001_rdg.manifest)
set_property(TEST ${name} APPEND PROPERTY LABELS quick)

set(name file-view)
set(test_name ${name}-test)
set(clean_name clean-${name})
add_executable(${test_name} file-view.cpp)
target_link_libraries(${test_name} katana_tsuba)
add_test(NAME ${name} COMMAND ${test_name} "${CMAKE_CURRENT_BINARY_DIR}/file-view-test-wd")
set_tests_properties(${name} PROPERTIES FIXTURES_REQUIRED file-view-ready LABELS quick)
add_test(NAME ${clean_name} COMMAND ${CMAKE_COMMAND} -E rm -rf "${CMAKE_CURRENT_BINARY_DIR}/file-view-test-wd")
set_tests_properties(${clean_name} PROPERTIES FIXTURES_SETUP file-view-ready LABELS quick)


set(name parquet)
set(test_name ${name}-test)
set(clean_name clean-${name})
add_executable(${test_name} parquet.cpp)
target_link_libraries(${test_name} katana_tsuba)
add_test(NAME ${name} COMMAND ${test_name} "${CMAKE_CURRENT_BINARY_DIR}/parquet-test-wd")
set_tests_properties(${name} PROPERTIES FIXTURES_REQUIRED parquet-ready LABELS quick)
add_test(NAME ${clean_name} COMMAND ${CMAKE_COMMAND} -E rm -rf "${CMAKE_CURRENT_BINARY_DIR}/parquet-test-wd")
set_tests_properties(${clean_name} PROPERTIES FIXTURES_SETUP parquet-ready LABELS quick)

## Storage Format Version Unstable Flag tests
set(unstable_rdg_path ${PROJECT_BINARY_DIR}/Testing/Temporary/unstable_rdg)
set(group ${name}-fixture)
set(name unstable-storage-format-version-flag-on)
set(test_name ${name}-test)
add_executable(${test_name} storage-format-version/unstable-storage-format-version-flag-on.cpp)
target_link_libraries(${test_name} katana_tsuba)
target_include_directories(${test_name} PRIVATE ../src)
add_test(NAME ${name} COMMAND ${test_name} ${RDG_LDBC_003} ${unstable_rdg_path})
set_tests_properties(${name} PROPERTIES
  ENVIRONMENT KATANA_ENABLE_EXPERIMENTAL=UnstableRDGStorageFormat)
set_tests_properties(${name} PROPERTIES FIXTURES_SETUP ${group})
set_property(TEST ${name} APPEND PROPERTY LABELS quick)

# depends on the flag-on test to create a valid unstable RDG
set(name unstable-storage-format-version-flag-off)
set(test_name ${name}-test)
add_executable(${test_name} storage-format-version/unstable-storage-format-version-flag-off.cpp)
target_link_libraries(${test_name} katana_tsuba)
target_include_directories(${test_name} PRIVATE ../src)
add_test(NAME ${name} COMMAND ${test_name} ${RDG_LDBC_003} ${unstable_rdg_path})
set_property(TEST ${name} APPEND PROPERTY FIXTURES_REQUIRED ${group})
set_property(TEST ${name} APPEND PROPERTY LABELS quick)

## Storage Format Version backwards compatibility tests ##

## storage format version 2
## storage format version 2 is not supported

## storage format version 3
## We can't do v1-v3 tests here in libtsuba because we depends on PropertyGraph to extract EntityTypeIDs from properties
## see libgalois/test/storage-format-version*

# Test optional topologies loading/storing cycle on v3 input

set(name storage-format-version-v3-v3-optional-topologies)
set(test_name ${name}-test)
add_executable(${test_name} storage-format-version/v3-optional-topologies.cpp)
target_link_libraries(${test_name} katana_tsuba)
target_include_directories(${test_name} PRIVATE ../src)
add_test(NAME ${name} COMMAND ${test_name} ${RDG_LDBC_003_V3})
set_property(TEST ${name} APPEND PROPERTY LABELS quick)
set_tests_properties(${name} PROPERTIES LABELS quick)

# Test entity type id loading/storing cycle on v3 input
set(name storage-format-version-v3-v3-uint16-entity-type-ids)
set(test_name ${name}-test)
add_executable(${test_name} storage-format-version/v3-uint16-entity-type-ids.cpp)
target_link_libraries(${test_name} katana_tsuba)
target_include_directories(${test_name} PRIVATE ../src)
add_test(NAME ${name} COMMAND ${test_name} ${RDG_LDBC_003_V3})

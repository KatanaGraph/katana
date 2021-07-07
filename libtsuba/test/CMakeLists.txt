add_executable(test_RDGPartHeader test_RDGPartHeader.cpp)
target_link_libraries(test_RDGPartHeader tsuba)
target_include_directories(test_RDGPartHeader PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../src>
)

set(md_formats json parquet)
foreach(format ${md_formats})
  set(header_path s3://katana-ci/tsuba-tests/rdg-headers/${format}_header)
  set(test_name test_${format}_rdgpartheader)
  add_test(NAME ${test_name}  COMMAND "$<TARGET_FILE:test_RDGPartHeader>" ${header_path})
  set_tests_properties(${test_name}
    PROPERTIES
      ENVIRONMENT KATANA_DO_NOT_BIND_THREADS=1)
  set_tests_properties(${test_name}
    PROPERTIES
    LABELS quick)
endforeach()



if(TSUBA_BACKEND_S3)
  if(APPLE)
    message(WARNING "skipping build of s3_bench because MacOS does not have memfd_create")
  else()
    add_executable(s3_bench s3_bench.cpp)
    target_link_libraries(s3_bench tsuba)
  endif()
endif()

add_executable(fo_bench fo_bench.cpp)
target_link_libraries(fo_bench tsuba)

add_executable(s3_pg_basics s3_pg_basics.cpp)
target_link_libraries(s3_pg_basics tsuba)

add_executable(tsuba_ll tsuba_ll.cpp)
target_link_libraries(tsuba_ll tsuba)

add_executable(tsuba_md5sum tsuba_md5sum.cpp md5.cpp)
target_link_libraries(tsuba_md5sum tsuba)

add_executable(tsuba_gc tsuba_gc.cpp)
target_link_libraries(tsuba_gc tsuba)

add_executable(tsuba_forget tsuba_forget.cpp)
target_link_libraries(tsuba_forget tsuba)

add_executable(tsuba_bench tsuba_bench.cpp)
target_link_libraries(tsuba_bench tsuba)

add_executable(tsuba_fault tsuba_fault.cpp)
target_link_libraries(tsuba_fault tsuba)

add_executable(tsuba_fault_runner tsuba_fault_runner.cpp)
target_link_libraries(tsuba_fault_runner tsuba)

add_executable(unit-fileobjects fileobjects.cpp)
target_link_libraries(unit-fileobjects tsuba)
add_test(NAME unit-fileobjects COMMAND unit-fileobjects)
set_tests_properties(unit-fileobjects PROPERTIES LABELS quick)

add_executable(unit-loadpartial loadpartial.cpp)
target_link_libraries(unit-loadpartial tsuba)
add_test(NAME unit-loadpartial COMMAND unit-loadpartial)

add_executable(tsuba_test tsuba_test.cpp md5.cpp)
target_link_libraries(tsuba_test tsuba)
add_test(NAME tsuba_test COMMAND tsuba_test s3://katana-ci/delete_me)
# new_path no longer needed, but instructive
# set(new_path "${CMAKE_BINARY_DIR}/bin:${CMAKE_CURRENT_BINARY_DIR}:$ENV{PATH}")
set_tests_properties(tsuba_test PROPERTIES ENVIRONMENT "AWS_DEFAULT_REGION=us-east-1")

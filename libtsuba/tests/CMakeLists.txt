function(add_test_unit name)
  set(options)
  set(multi_value_args REQUIRES COMMAND_PREFIX)
  cmake_parse_arguments(X "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  foreach(required ${X_REQUIRES})
    if(${${required}} MATCHES "TRUE")
    else()
      message(STATUS "NOT compiling ${name} (missing: ${required})")
      return()
    endif()
  endforeach()

  set(test_name unit-${name})

  add_executable(${test_name} ${name}.cpp)
  target_link_libraries(${test_name} galois_shmem lonestar)

  set(commandline ${X_COMMAND_PREFIX})
  list(APPEND commandline "$<TARGET_FILE:${test_name}>")
  list(APPEND commandline ${X_UNPARSED_ARGUMENTS})

  add_test(NAME ${test_name} COMMAND ${commandline})

  # Allow parallel tests
  set_tests_properties(${test_name}
    PROPERTIES
      ENVIRONMENT GALOIS_DO_NOT_BIND_THREADS=1
      LABELS quick
    )
endfunction()

add_executable(s3_bench s3_bench)
target_link_libraries(s3_bench tsuba)

add_executable(s3_pg_basics s3_pg_basics)
target_link_libraries(s3_pg_basics tsuba)

add_executable(tsuba_mkfile tsuba_mkfile)
target_link_libraries(tsuba_mkfile tsuba)

add_executable(tsuba_cp tsuba_cp)
target_link_libraries(tsuba_cp tsuba)

add_executable(tsuba_ll tsuba_ll)
target_link_libraries(tsuba_ll tsuba)

add_executable(tsuba_md5sum tsuba_md5sum md5)
target_link_libraries(tsuba_md5sum tsuba)

add_executable(tsuba_rm tsuba_rm)
target_link_libraries(tsuba_rm tsuba)

if(GALOIS_ENABLE_DIST)
  add_executable(tsuba_dist_write tsuba_dist_write)
  target_link_libraries(tsuba_dist_write tsuba)
  target_link_libraries(tsuba_dist_write MPI::MPI_CXX)
endif()


add_test_unit(fileobjects)
# Don't make it a unit test, nor make it quick
#   because can't run s3 tests with current CI
#add_test_unit(tsuba_basic_quick)
add_executable(tsuba_basic_quick tsuba_basic_quick)
target_link_libraries(tsuba_basic_quick tsuba)


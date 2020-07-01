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

add_test_unit(fileobjects)

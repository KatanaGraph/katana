function(add_test_unit name)
  set(options NOT_QUICK)
  set(multi_value_args REQUIRES)
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
  target_link_libraries(${test_name} galois_shmem)

  list(APPEND command_line "$<TARGET_FILE:${test_name}>")
  list(APPEND command_line ${X_UNPARSED_ARGUMENTS})

  add_test(NAME ${test_name} COMMAND ${command_line})

  set_tests_properties(${test_name}
    PROPERTIES
      ENVIRONMENT GALOIS_DO_NOT_BIND_THREADS=1)

  # Allow parallel tests
  if(NOT X_NOT_QUICK)
    set_tests_properties(${test_name}
      PROPERTIES
        LABELS quick
      )
  endif()
endfunction()

add_test_unit(acquire)
add_test_unit(bandwidth)
add_test_unit(barriers 1024 2)
add_test_unit(empty-member-lcgraph)
add_test_unit(flatmap)
add_test_unit(floating-point-errors)
add_test_unit(foreach)
add_test_unit(forward-declare-graph)
add_test_unit(gcollections)
add_test_unit(graph)
add_test_unit(graph-compile)
add_test_unit(gslist)
add_test_unit(hwtopo)
add_test_unit(lock)
add_test_unit(loop-overhead REQUIRES OPENMP_FOUND)
add_test_unit(mem)
add_test_unit(morph-graph)
add_test_unit(morph-graph-removal)
add_test_unit(move)
add_test_unit(offset)
add_test_unit(oneach)
add_test_unit(papi 2)
add_test_unit(range)
add_test_unit(pc)
add_test_unit(property-file-graph)
add_test_unit(property-graph)
add_test_unit(property-graph-bench NOT_QUICK)
add_test_unit(reduction)
add_test_unit(sort)
add_test_unit(static)
add_test_unit(traits)
add_test_unit(two-level-iterator)
add_test_unit(wakeup-overhead)
add_test_unit(worklists-compile)

target_link_libraries(unit-wakeup-overhead LLVMSupport)

target_link_libraries(unit-property-graph-bench benchmark::benchmark)

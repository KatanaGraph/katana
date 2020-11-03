set(RESULT_CHECKER ${PROJECT_SOURCE_DIR}/scripts/result_checker.py)

function(add_test_scale type app)
  set(options NOT_QUICK)
  set(one_value_args)
  set(multi_value_args)
  cmake_parse_arguments(X "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  set(threads)
  set(thr "${GALOIS_NUM_TEST_THREADS}")
  while (${thr} GREATER 1)
    list(APPEND threads ${thr})
    math(EXPR thr "${thr} / 2")
  endwhile()
  list(APPEND threads "1")

  foreach (thr ${threads})
    set(name run-${app}.input=${type}.threads=${thr})
    add_test(NAME ${name} COMMAND ${app} ${X_UNPARSED_ARGUMENTS} -t ${thr})
    if(NOT X_NOT_QUICK)
      # Allow parallel tests
      set_tests_properties(${name}
        PROPERTIES ENVIRONMENT GALOIS_DO_NOT_BIND_THREADS=1 LABELS quick)
    endif()
  endforeach()
endfunction(add_test_scale)

add_subdirectory(liblonestar)

add_subdirectory(analytics)
add_subdirectory(tutorial_examples)

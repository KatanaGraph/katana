set(RESULT_CHECKER ${PROJECT_SOURCE_DIR}/scripts/result_checker.py)


# Return canonical name of an application, e.g., pagerank-pull-cpu => pagerank-pull
function(stem_app app output)
	string(REPLACE "-" ";" app_list ${app})
	list(POP_BACK app_list)
	list(JOIN app_list "-" result)

	set(${output} ${result} PARENT_SCOPE)
endfunction()

function(add_test_scale type app)
  set(no_value_options NOT_QUICK NO_VERIFY)
  set(single_value_options INPUT INPUT_URI)
  set(multi_value_options)
  cmake_parse_arguments(X "${no_value_options}" "${single_value_options}" "${multi_value_options}" ${ARGN})

  set(threads)
  set(thr "${KATANA_NUM_TEST_THREADS}")
  stem_app(${app} app_id)
  set(verification_log ${BASE_VERIFICATION}/verify/${app_id}.${X_INPUT})
  while (${thr} GREATER 1)
    list(APPEND threads ${thr})
    math(EXPR thr "${thr} / 2")
  endwhile()
  list(APPEND threads "1")

  foreach (thr ${threads})
    set(suffix -${app}.input=${type}.threads=${thr})
    set(command ${app} ${X_INPUT_URI} ${X_UNPARSED_ARGUMENTS} -t ${thr})
    set(output_location ${PROJECT_BINARY_DIR}/Testing/verify/run${suffix})
    if(NOT X_NO_VERIFY)
	    list(APPEND command -output -outputLocation=${output_location})
    endif()

    add_test(NAME setup-clean${suffix}
      COMMAND ${CMAKE_COMMAND} -E rm -rf ${output_location})
    add_test(NAME setup-make${suffix}
      COMMAND ${CMAKE_COMMAND} -E make_directory ${output_location})

    add_test(NAME run${suffix} COMMAND ${command})

    if(X_NO_VERIFY)
      add_test(NAME verify${suffix}
        COMMAND ${CMAKE_COMMAND} -E true)
    else()
      add_test(NAME verify${suffix}
        COMMAND python3 ${RESULT_CHECKER} -t=0.01 -m=0.001 -sort=1 ${verification_log} ${output_location})
    endif()

    if(NOT X_NOT_QUICK)
      # Allow parallel tests
      set_tests_properties(setup-make${suffix} setup-clean${suffix}
      PROPERTIES
        FIXTURES_SETUP run${suffix})
      set_tests_properties(setup-make${suffix}
        PROPERTIES
          DEPENDS setup-clean${suffix})
      set_tests_properties(run${suffix}
        PROPERTIES
          FIXTURES_REQUIRED run${suffix}
          FIXTURES_SETUP verify${suffix}
          ENVIRONMENT KATANA_DO_NOT_BIND_THREADS=1 LABELS quick)
      set_tests_properties(verify${suffix}
        PROPERTIES
          DEPENDS run${suffix}
          FIXTURES_REQUIRED verify${suffix}
          LABELS quick)
    endif()
  endforeach()
endfunction(add_test_scale)

add_subdirectory(liblonestar)

add_subdirectory(analytics)
add_subdirectory(tutorial_examples)

function(add_unit_test name)
  set(test_name ${name}-test)

  add_executable(${test_name} ${name}.cpp)
  target_link_libraries(${test_name} katana_support Threads::Threads)

  set(command_line "$<TARGET_FILE:${test_name}>")

  add_test(NAME ${test_name} COMMAND ${command_line})

  # Allow parallel tests
  set_tests_properties(${test_name}
    PROPERTIES
      ENVIRONMENT KATANA_DO_NOT_BIND_THREADS=1
      LABELS quick
    )
endfunction()

add_unit_test(arrow)
add_unit_test(bitmath)
add_unit_test(cache)
add_unit_test(env)
add_unit_test(logging)
add_unit_test(opaque-id)
add_unit_test(random)
add_unit_test(result)
add_unit_test(signals)
add_unit_test(strings)
add_unit_test(tracing)
add_unit_test(type-manager)
add_unit_test(uri)
add_unit_test(zip_iterator)

add_executable(arrow-bench arrow-bench.cpp)
target_link_libraries(arrow-bench katana_support benchmark::benchmark)
add_test(NAME arrow-bench COMMAND arrow-bench --benchmark_filter=/1024)
set_tests_properties(arrow-bench PROPERTIES LABELS quick)

add_executable(result-bench result-bench.cpp)
target_link_libraries(result-bench katana_support benchmark::benchmark)
add_test(NAME result-bench COMMAND result-bench --benchmark_filter=KatanaResultWithContext/1/1024/3/16)
set_tests_properties(result-bench PROPERTIES LABELS quick)

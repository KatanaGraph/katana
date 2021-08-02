function(add_unit_test name)
  set(test_name ${name}-test)
  add_executable(${test_name} ${name}.cpp)
  add_dependencies(${test_name} katana_disable_dlclose)
  target_link_libraries(${test_name} PRIVATE "-ldl")
  set(command_line "$<TARGET_FILE:${test_name}>")
  add_test(NAME ${test_name} COMMAND ${command_line})
  # Allow parallel tests
  set_property(TEST ${test_name} APPEND
    PROPERTY ENVIRONMENT KATANA_DO_NOT_BIND_THREADS=1)
  set_property(TEST ${test_name} APPEND
    PROPERTY ENVIRONMENT LD_PRELOAD=${KATANA_LIBASAN_PATH}:$<TARGET_FILE:katana_disable_dlclose>)
  set_property(TEST ${test_name} APPEND
    PROPERTY LABELS quick)
endfunction()

add_unit_test(no_module_unload)

# args:
# NOT_QUICK - flag to mark this test as not quick
# REQUIRES - list of variables that must be true to compile this test
# LINK_LIBRARIES - list of library names to link
function(add_test_unit name)
  set(no_value_options NOT_QUICK)
  set(multi_value_options REQUIRES)
  set(multi_value_options REQUIRES LINK_LIBRARIES)
  cmake_parse_arguments(X "${no_value_options}" "" "${multi_value_options}" ${ARGN})

  foreach(required ${X_REQUIRES})
    if(${${required}} MATCHES "TRUE")
    else()
      message(STATUS "NOT compiling ${name} (missing: ${required})")
      return()
    endif()
  endforeach()

  set(test_name ${name}-test)

  add_executable(${test_name} ${name}.cpp)
  target_link_libraries(${test_name} katana_galois)
  # TODO(amber): this is a bit of an ugly hack. tests should specify which of
  # katana_galois and katana_graph they want to link to
  if (TARGET katana_graph)
    target_link_libraries(${test_name} katana_graph)
  endif()
  if(X_LINK_LIBRARIES)
    target_link_libraries(${test_name} ${X_LINK_LIBRARIES})
  endif()

  list(APPEND command_line "$<TARGET_FILE:${test_name}>")
  list(APPEND command_line ${X_UNPARSED_ARGUMENTS})

  add_test(NAME ${test_name} COMMAND ${command_line})

  set_tests_properties(${test_name}
    PROPERTIES
      ENVIRONMENT KATANA_DO_NOT_BIND_THREADS=1)

  # Allow parallel tests
  if(NOT X_NOT_QUICK)
    set_tests_properties(${test_name}
      PROPERTIES
        LABELS quick
      )
  endif()
endfunction()

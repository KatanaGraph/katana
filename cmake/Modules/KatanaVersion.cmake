function(katana_get_version_component COMPONENT)
  set(no_value_options)
  set(one_value_options)
  set(multi_value_options OUTPUT)
  cmake_parse_arguments(_args "${no_value_options}" "${one_value_options}" "${multi_value_options}" ${ARGN})
  execute_process(COMMAND ${CMAKE_CURRENT_LIST_DIR}/../../scripts/version show --${COMPONENT}
      OUTPUT_STRIP_TRAILING_WHITESPACE
      OUTPUT_VARIABLE version_output
      RESULT_VARIABLE result
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  if (result OR version_output STREQUAL "")
    message(FATAL_ERROR "Failed to get version ${COMPONENT}: ${result}, ${version_output}")
  endif()
  foreach(out_var IN ITEMS ${_args_OUTPUT})
    set(${out_var} ${version_output} PARENT_SCOPE)
  endforeach()
endfunction()

set(OLD_KATANA_VERSION ${KATANA_VERSION})

katana_get_version_component(full OUTPUT KATANA_VERSION ${PROJECT_NAME}_VERSION)

if(OLD_KATANA_VERSION AND NOT (OLD_KATANA_VERSION STREQUAL KATANA_VERSION))
  message(FATAL_ERROR "Version for project ${PROJECT_NAME} does not match version for previous project (maybe ${CMAKE_PROJECT_NAME}): ${OLD_KATANA_VERSION} != ${KATANA_VERSION}")
endif()

katana_get_version_component(major OUTPUT KATANA_VERSION_MAJOR ${PROJECT_NAME}_VERSION_MAJOR)
katana_get_version_component(minor OUTPUT KATANA_VERSION_MINOR ${PROJECT_NAME}_VERSION_MINOR)
katana_get_version_component(patch OUTPUT KATANA_VERSION_PATCH ${PROJECT_NAME}_VERSION_PATCH)
katana_get_version_component(local OUTPUT KATANA_VERSION_LOCAL ${PROJECT_NAME}_VERSION_LOCAL)

message(STATUS "${PROJECT_NAME} version: ${${PROJECT_NAME}_VERSION}")

# TODO(amp): Ideally this would cause a cmake reconfiguration when version information changes (including git commits)

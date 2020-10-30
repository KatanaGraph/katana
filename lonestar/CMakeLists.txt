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

function(app_gpu target_name)
  string(CONCAT target_name ${target_name} "-gpu")
  add_executable(${target_name} ${ARGN})
  install(TARGETS ${target_name} DESTINATION "${CMAKE_INSTALL_BINDIR}" EXCLUDE_FROM_ALL)
  target_compile_options(${target_name} PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:-w>)
  target_link_libraries(${target_name} Galois::gpu)
  set_property(TARGET ${target_name} PROPERTY CUDA_STANDARD 14)
endfunction()

function(add_test_gpu app input output baseoutputext)
  set(options NOT_QUICK)
  set(one_value_args)
  set(multi_value_args)
  set(suffix "-${app}-${input}-gpu")
  cmake_parse_arguments(X "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  # TODO: add verification log support

  string(REPLACE "_" ";" app_list ${app})
  list(GET app_list 0 app_id)

  set(name run-${app}-gpu.input=${input})
  add_test(NAME ${name} COMMAND ${app}-gpu ${X_UNPARSED_ARGUMENTS})

  if(NOT X_NOT_QUICK)
    set_tests_properties(${name} PROPERTIES LABELS quick)
  endif()
endfunction(add_test_gpu)

# Return canonical name of an application, e.g., pagerank-pull-dist => pagerank-pull
function(stem_app app output)
  string(REPLACE "-" ";" app_list ${app})
  list(POP_BACK app_list)
  list(JOIN app_list "-" result)

  set(${output} ${result} PARENT_SCOPE)
endfunction()


if(GALOIS_ENABLE_DIST)
  add_subdirectory(libdistbench)

  if(GALOIS_ENABLE_GPU)
    # turn on cuda for distbench as well
    target_compile_definitions(distbench PRIVATE GALOIS_ENABLE_GPU=1)

    # for debugging
    add_definitions(-DGALOIS_CUDA_CHECK_ERROR)
    if(CMAKE_BUILD_TYPE MATCHES "Debug")
      add_compile_options("$<$<COMPILE_LANGUAGE:CUDA>:-lineinfo>")
    endif()
  endif()

  function(app_dist name target_name)
    set(options NO_GPU)
    set(one_value_args)
    set(multi_value_args)
    cmake_parse_arguments(X "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
    string(CONCAT target_name ${target_name} "-dist")

    FILE(GLOB CPPSOURCES ${name}*.cpp)
    add_executable(${target_name} ${CPPSOURCES})
    add_dependencies(apps ${target_name})
    target_link_libraries(${target_name} Galois::shmem LLVMSupport)
    install(TARGETS ${target_name} DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT apps EXCLUDE_FROM_ALL)

    target_link_libraries(${target_name} distbench)
    if(GALOIS_PER_ROUND_STATS)
      target_compile_definitions(${target_name} PRIVATE GALOIS_PER_ROUND_STATS=1)
    endif()
    if(GALOIS_COMM_STATS)
      target_compile_definitions(${target_name} PRIVATE GALOIS_COMM_STATS=1)
    endif()

    if(GALOIS_ENABLE_GPU AND NOT X_NO_GPU)
      target_compile_definitions(${target_name} PRIVATE GALOIS_ENABLE_GPU=1)
      target_link_libraries(${target_name} ${target_name}_cuda)

      FILE(GLOB CUSOURCES ${name}*.cu)
      add_library(${target_name}_cuda ${CUSOURCES})
      target_compile_options(${target_name}_cuda PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:-w>)
      target_link_libraries(${target_name}_cuda Galois::gpu)
      set_property(TARGET ${target_name}_cuda PROPERTY CUDA_STANDARD 14)
    endif()
  endfunction()

  function(add_test_dist_and_verify app type part N np)
    set(options GPU NOT_QUICK NO_VERIFY STORE_PART)
    set(one_value_args INPUT INPUT_URI)
    set(multi_value_args)
    cmake_parse_arguments(X "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

    math(EXPR threads "(${N} / ${np})")

    stem_app(${app} app_id)
    set(verification_log ${BASE_VERIFICATION}/verify/${app_id}.${X_INPUT})

    if (${X_STORE_PART})
      set(suffix "-${app}-store-part.input=${X_INPUT}.mode=${type}.policy=${part}.partitions=${N}.hosts=${np}")
    else()
      set(suffix "-${app}.input=${X_INPUT}.mode=${type}.policy=${part}.partitions=${N}.hosts=${np}")
    endif()

    set(output_location ${PROJECT_BINARY_DIR}/Testing/verify/run${suffix})

    if (${X_STORE_PART})
      set(partitioner_input ${X_INPUT_URI})
      set(X_INPUT_URI ${output_location}/part_graph)
    endif()

    set(command mpiexec --bind-to none -n ${np} $<TARGET_FILE:${app}> ${X_INPUT_URI} ${X_UNPARSED_ARGUMENTS} -t=${threads} -partition=${part})

    if(NOT X_NO_VERIFY)
      list(APPEND command -output -outputLocation=${output_location})
    endif()

    add_test(NAME setup-clean${suffix}
      COMMAND ${CMAKE_COMMAND} -E remove_directory ${output_location})
    add_test(NAME setup-make${suffix}
      COMMAND ${CMAKE_COMMAND} -E make_directory ${output_location})

    if (${X_STORE_PART})
      add_test(NAME setup-unlink${suffix}
        COMMAND $<TARGET_FILE:tsuba_forget> -f ${X_INPUT_URI})
      add_test(NAME setup-partition${suffix}
        COMMAND mpiexec --bind-to none -n ${np} $<TARGET_FILE:partition-dist> -newRDG=${X_INPUT_URI} ${partitioner_input} --edgePropertyName=value -t=${threads} -partition=${part} --loadAllProperties --persistAllProperties)
    else()
      add_test(NAME setup-partition${suffix} COMMAND ${CMAKE_COMMAND} -E true)
      add_test(NAME setup-unlink${suffix} COMMAND ${CMAKE_COMMAND} -E true)
    endif()

    add_test(NAME run${suffix} COMMAND ${command})

    if(X_NO_VERIFY)
      add_test(NAME verify${suffix}
        COMMAND ${CMAKE_COMMAND} -E true)
    else()
      add_test(NAME verify${suffix}
        COMMAND python3 ${RESULT_CHECKER} -t=0.01 -sort=1 ${verification_log} ${output_location})
    endif()

    # With fixtures, when ctest is given part of a test suite to run, it will
    # automatically include any other FIXTURE_REQUIRED fixtures. A fixture is
    # is just a collection of tests with the same FIXTURES_SETUP property. The
    # fixture name is just an arbitrary string
    set_tests_properties(setup-make${suffix} setup-clean${suffix}
        setup-unlink${suffix}
      PROPERTIES
        FIXTURES_SETUP setup${suffix})
    set_tests_properties(setup-make${suffix}
      PROPERTIES
        DEPENDS setup-clean${suffix})
    set_tests_properties(setup-unlink${suffix}
      PROPERTIES
        DEPENDS setup-make${suffix})
    set_tests_properties(setup-partition${suffix}
      PROPERTIES
        FIXTURES_SETUP setup${suffix}
        DEPENDS setup-unlink${suffix}
        # OpenMPI triggers a lot of leak warnings. Disable it during
        # sanitizer builds for now.
        ENVIRONMENT "GALOIS_DO_NOT_BIND_THREADS=1;ASAN_OPTIONS=detect_leaks=0")
    set_tests_properties(run${suffix}
      PROPERTIES
        DEPENDS setup-partition${suffix}
        FIXTURES_REQUIRED setup${suffix}
        FIXTURES_SETUP ${suffix}
        # OpenMPI triggers a lot of leak warnings. Disable it during
        # sanitizer builds for now.
        ENVIRONMENT "GALOIS_DO_NOT_BIND_THREADS=1;ASAN_OPTIONS=detect_leaks=0")
    set_tests_properties(verify${suffix}
      PROPERTIES
        DEPENDS run${suffix}
        FIXTURES_REQUIRED ${suffix})

    if(X_GPU)
      set_tests_properties(run${suffix} PROPERTIES RUN_SERIAL true)
    endif()

    if(NOT X_NOT_QUICK)
      set_tests_properties(verify${suffix} PROPERTIES LABELS quick)
    endif()
  endfunction()

  function(add_test_dist_for_partitions app type num_threads num_gpus part)
    # use at most 8 threads during testing
    if(${num_threads} GREATER 8)
      set(num_threads 8)
    endif()

    # always use 4 partitions
    set(partitions 4)
    # get 2 and 1 into partitions set; code remains in case we want to change
    # 4 to something else
    set(other_parts ${partitions})
    while (${other_parts} GREATER 1)
      math(EXPR other_parts "${other_parts} / 2")
      list(APPEND partitions ${other_parts})
    endwhile()
    list(REVERSE partitions)

    foreach(np ${partitions})
      add_test_dist_and_verify(${app} ${type}-cpu ${part} ${num_threads} ${np} ${ARGN})
    endforeach()

    if(NOT GALOIS_ENABLE_GPU)
      return()
    endif()

    if(num_gpus LESS_EQUAL 0)
      return()
    endif()

    if(num_gpus GREATER_EQUAL num_threads)
      message(FATAL_ERROR "number of test gpus (${num_gpus}) should be less than number of test threads (${num_threads})")
    endif()

    set(PSET "-pset=")
    foreach(np RANGE 1 ${num_gpus})
      set(PSET "${PSET}g")
      add_test_dist_and_verify(${app} ${type}-gpu ${part} ${num_threads} ${np} GPU ${ARGN} -num_nodes=1 ${PSET})
    endforeach(np)
    set(PSET "${PSET}c")
    math(EXPR np "(${G} + 1)")
    add_test_dist_and_verify(${app} ${type}-cpugpu ${part} ${num_threads} ${np} GPU ${ARGN} -num_nodes=1 ${PSET} -scalegpu=3)
  endfunction()

  function(add_test_dist app)
    set(options NO_GPU NO_ASYNC)
    set(one_value_args)
    set(multi_value_args)
    cmake_parse_arguments(X "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

    set(num_gpus ${GALOIS_NUM_TEST_GPUS})
    if(X_NO_GPU)
      set(num_gpus 0)
    endif()

    set(num_threads ${GALOIS_NUM_TEST_THREADS})

    ## NOTE: Restricting to limited set of policies on push-style algorithms for property-graphs for now.
    foreach (part blocked-oec blocked-iec blocked-cvc blocked-hvc fennel-oec)
      if(NOT ${X_NO_ASYNC})
        add_test_dist_for_partitions(${app} sync ${num_threads} ${num_gpus} ${part} ${X_UNPARSED_ARGUMENTS} -exec=Sync)
        add_test_dist_for_partitions(${app} async ${num_threads} ${num_gpus} ${part} ${X_UNPARSED_ARGUMENTS} -exec=Async)
      else()
        add_test_dist_for_partitions(${app} sync ${num_threads} ${num_gpus} ${part} ${X_UNPARSED_ARGUMENTS})
      endif()
    endforeach()
  endfunction()
endif()


add_subdirectory(liblonestar)

add_subdirectory(tutorial_examples)

add_subdirectory(analytics)
add_subdirectory(eda)
add_subdirectory(mining)
add_subdirectory(scientific)
add_subdirectory(querying)

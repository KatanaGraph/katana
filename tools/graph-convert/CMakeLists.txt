add_executable(graph-convert graph-convert.cpp)
target_link_libraries(graph-convert galois_shmem LLVMSupport)
install(TARGETS graph-convert
  EXPORT GaloisTargets
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  COMPONENT tools
)

function(compare_with_sample test_arg compare_arg input expected)
  set(suffix ${test_arg}${compare_arg}-${input})

  get_filename_component(base_input ${input} NAME)

  add_test(NAME create${suffix}
    COMMAND graph-convert ${test_arg} ${CMAKE_CURRENT_SOURCE_DIR}/${input} ${base_input}.test
  )

  add_test(NAME convert${suffix}
    COMMAND graph-convert ${compare_arg} ${base_input}.test ${base_input}.compare
  )

  add_test(NAME compare${suffix}
    COMMAND ${CMAKE_COMMAND} -E compare_files ${base_input}.compare ${CMAKE_CURRENT_SOURCE_DIR}/${expected}
  )

  set_tests_properties(create${suffix}
    PROPERTIES
      FIXTURES_SETUP create${suffix})

  set_tests_properties(convert${suffix}
    PROPERTIES
      DEPENDS create${suffix}
      FIXTURES_REQUIRED create${suffix}
      FIXTURES_SETUP convert${suffix})

  set_tests_properties(compare${suffix}
    PROPERTIES
      LABELS quick
      DEPENDS convert${suffix}
      FIXTURES_REQUIRED convert${suffix})
endfunction()

compare_with_sample(-edgelist2gr -gr2edgelist test-inputs/with-blank-lines.edgelist test-inputs/with-blank-lines.edgelist.expected)
compare_with_sample(-csv2gr -gr2edgelist test-inputs/sample.csv test-inputs/with-blank-lines.edgelist.expected)
compare_with_sample(-edgelist2gr -gr2edgelist test-inputs/with-comments.edgelist test-inputs/with-comments.edgelist.expected)

add_executable(graph-convert-huge graph-convert-huge.cpp)
target_link_libraries(graph-convert-huge galois_shmem LLVMSupport)
if (TARGET Boost::Boost)
  target_link_libraries(graph-convert-huge Boost::Boost)
else()
  target_link_libraries(graph-convert-huge Boost::iostreams)
endif()
install(TARGETS graph-convert-huge
  EXPORT GaloisTargets
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  COMPONENT tools
)

find_package(LibXml2 2.9.1 REQUIRED)
find_package(date REQUIRED)
find_package(mongocxx REQUIRED)

add_library(graph-properties-convert-common STATIC graph-properties-convert.cpp Transforms.cpp)
target_include_directories(graph-properties-convert-common PUBLIC .)
target_link_libraries(graph-properties-convert-common PRIVATE LibXml2::LibXml2)
target_link_libraries(graph-properties-convert-common PUBLIC galois_shmem date::date)
target_link_libraries(graph-properties-convert-common PRIVATE mongo::mongocxx_shared)

add_executable(graph-properties-convert graph-properties-convert-main.cpp)
target_link_libraries(graph-properties-convert PRIVATE LLVMSupport)
target_link_libraries(graph-properties-convert PRIVATE graph-properties-convert-common)
install(TARGETS graph-properties-convert
  EXPORT GaloisTargets
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  COMPONENT tools
)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
  add_subdirectory(tests)
endif()

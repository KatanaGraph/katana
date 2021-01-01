add_executable(graph-convert graph-convert.cpp)
target_link_libraries(graph-convert katana_galois LLVMSupport)
install(TARGETS graph-convert
  EXPORT KatanaTargets
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
target_link_libraries(graph-convert-huge katana_galois LLVMSupport)
if (TARGET Boost::Boost)
  target_link_libraries(graph-convert-huge Boost::Boost)
else()
  target_link_libraries(graph-convert-huge Boost::iostreams)
endif()
install(TARGETS graph-convert-huge
  EXPORT KatanaTargets
  COMPONENT tools
)

add_library(graph-properties-convert-common STATIC)
add_executable(graph-properties-convert)

find_package(LibXml2 2.9.1 REQUIRED)
find_package(date REQUIRED)

set(sources
  graph-properties-convert-schema.cpp
  graph-properties-convert-graphml.cpp
  Transforms.cpp
)

if(mongoc-1.0_FOUND)
  list(APPEND sources graph-properties-convert-mongodb.cpp)
  target_link_libraries(graph-properties-convert-common PUBLIC katana_galois mongo::mongoc_static)
  target_compile_definitions(graph-properties-convert-common PUBLIC KATANA_MONGOC_FOUND)
endif()
if(MySQL_FOUND)
  list(APPEND sources graph-properties-convert-mysql.cpp)
  target_link_libraries(graph-properties-convert-common PUBLIC katana_galois MySQL::MySQL)
  target_compile_definitions(graph-properties-convert-common PUBLIC KATANA_MYSQL_FOUND)
endif()

target_sources(graph-properties-convert-common PRIVATE ${sources})
target_include_directories(graph-properties-convert-common PUBLIC .)
target_link_libraries(graph-properties-convert-common PUBLIC LibXml2::LibXml2)
target_link_libraries(graph-properties-convert-common PUBLIC date::date)
target_link_libraries(graph-properties-convert-common PUBLIC katana_galois)

target_sources(graph-properties-convert PRIVATE graph-properties-convert-main.cpp)
target_link_libraries(graph-properties-convert PRIVATE LLVMSupport)
target_link_libraries(graph-properties-convert PRIVATE graph-properties-convert-common)
target_link_libraries(graph-properties-convert PUBLIC katana_galois)

add_executable(oplog-rdg oplog-rdg.cpp)
target_link_libraries(oplog-rdg graph-properties-convert-common katana_galois)

install(TARGETS graph-properties-convert
  EXPORT KatanaTargets
  COMPONENT tools
)

if(KATANA_IS_MAIN_PROJECT AND BUILD_TESTING)
  add_subdirectory(tests)
endif()

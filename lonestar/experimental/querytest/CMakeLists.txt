app(querytest querytest.cpp)

target_include_directories(querytest PUBLIC
  ${CMAKE_SOURCE_DIR}/lonestar/experimental/python
  ${CMAKE_SOURCE_DIR}/lonestar/experimental/graphsimulation
)

target_link_libraries(querytest galois_python)

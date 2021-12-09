add_executable(k-core-cpu kcore_cli.cpp)
add_dependencies(apps k-core-cpu)
target_link_libraries(k-core-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small k-core-cpu INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_symmetric" --kCoreNumber=100 -symmetricGraph --algo=Synchronous)

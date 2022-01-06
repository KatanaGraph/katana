add_executable(k-core-cpu kcore_cli.cpp)
add_dependencies(apps k-core-cpu)
target_link_libraries(k-core-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small k-core-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT15_SYMMETRIC}" --kCoreNumber=100 -symmetricGraph --algo=Synchronous)
add_test_scale(small k-core-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT15_SYMMETRIC}" --kCoreNumber=100 -symmetricGraph --algo=Asynchronous)

add_test_scale(small k-core-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --kCoreNumber=100 --algo=Synchronous)
add_test_scale(small k-core-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT15}" --kCoreNumber=100 --algo=Asynchronous)

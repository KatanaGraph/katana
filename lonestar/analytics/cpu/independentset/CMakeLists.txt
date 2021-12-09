add_executable(independentset-cpu independent_set_cli.cpp)
add_dependencies(apps independentset-cpu)
target_link_libraries(independentset-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small independentset-cpu INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat10_symmetric" NO_VERIFY "--algo=Priority" "--symmetricGraph")

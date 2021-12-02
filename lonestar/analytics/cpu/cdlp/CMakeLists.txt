add_executable(cdlp-cpu cdlp_cli.cpp)
add_dependencies(apps cdlp-cpu)
target_link_libraries(cdlp-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small cdlp-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" "-algo=Synchronous")

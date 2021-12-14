add_executable(cdlp-cpu cdlp_cli.cpp)
add_dependencies(apps cdlp-cpu)
target_link_libraries(cdlp-cpu PRIVATE Katana::galois lonestar)


add_executable(preflowpush-cpu Preflowpush.cpp)
add_dependencies(apps preflowpush-cpu)
target_link_libraries(preflowpush-cpu PRIVATE Katana::galois lonestar)
add_test_scale(small1 preflowpush-cpu INPUT torus5 INPUT_URI "${BASEINPUT}/reference/structured/torus5.gr" NO_VERIFY "-sourceNode=0" "-sinkNode=10")

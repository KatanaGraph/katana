add_executable(minimum-spanningtree-cpu Boruvka.cpp)
add_dependencies(apps minimum-spanningtree-cpu)
target_link_libraries(minimum-spanningtree-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 minimum-spanningtree-cpu INPUT rmat10 INPUT_URI "${MISC_TEST_DATASETS}/scalefree/rmat10.gr" NO_VERIFY)
add_test_scale(small2 minimum-spanningtree-cpu INPUT rome99 INPUT_URI "${MISC_TEST_DATASETS}/reference/structured/rome99.gr" NO_VERIFY)

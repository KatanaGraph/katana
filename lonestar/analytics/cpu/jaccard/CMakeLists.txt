add_executable(jaccard-cpu jaccard_cli.cpp)
add_dependencies(apps jaccard-cpu)
target_link_libraries(jaccard-cpu PRIVATE Katana::galois lonestar)

# add_test_scale(small1 jaccard-cpu "${MISC_TEST_DATASETS}/reference/structured/rome99.gr")
add_test_scale(small2 jaccard-cpu INPUT rmat15 INPUT_URI "${RDG_RMAT15_CLEANED_SYMMETRIC}" NO_VERIFY)

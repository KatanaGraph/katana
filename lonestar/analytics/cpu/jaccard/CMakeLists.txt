add_executable(jaccard-cpu jaccard_cli.cpp)
add_dependencies(apps jaccard-cpu)
target_link_libraries(jaccard-cpu PRIVATE Katana::galois lonestar)

# add_test_scale(small1 jaccard-cpu "${BASEINPUT}/reference/structured/rome99.gr")
add_test_scale(small2 jaccard-cpu INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NO_VERIFY)

add_executable(gmetis-cpu Coarsening.cpp GMetis.cpp Metric.cpp Partitioning.cpp Refine.cpp)
add_dependencies(apps gmetis-cpu)
target_link_libraries(gmetis-cpu PRIVATE Katana::galois lonestar)

# Disable failing test (issue #116).
add_test_scale(small1 gmetis-cpu "${MISC_TEST_DATASETS}/reference/structured/rome99.gr" "-numPartitions=4" NOT_QUICK NO_VERIFY)
add_test_scale(small2 gmetis-cpu "${MISC_TEST_DATASETS}/scalefree/rmat10.gr" "-numPartitions=256" NO_VERIFY)

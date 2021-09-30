add_executable(gmetis-cpu Coarsening.cpp GMetis.cpp Metric.cpp Partitioning.cpp Refine.cpp)
add_dependencies(apps gmetis-cpu)
target_link_libraries(gmetis-cpu PRIVATE Katana::galois lonestar)

# Disable failing test (issue #116).
add_test_scale(small1 gmetis-cpu "${BASEINPUT}/reference/structured/rome99.gr" "-numPartitions=4" NO_CI NO_VERIFY)
add_test_scale(small2 gmetis-cpu "${BASEINPUT}/scalefree/rmat10.gr" "-numPartitions=256" NO_VERIFY)

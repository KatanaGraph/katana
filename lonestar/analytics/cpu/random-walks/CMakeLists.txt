add_executable(random-walk-cpu random_walks_cli.cpp)
add_dependencies(apps random-walk-cpu)
target_link_libraries(random-walk-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small random-walk-cpu NO_VERIFY INPUT rmat10 INPUT_URI "${RDG_RMAT10_SYMMETRIC}" "-symmetricGraph" "-algo=Node2Vec" "-walkLength=3")

add_executable(matrixcompletion-sgd-cpu matrix_completion_sgd_cli.cpp)
add_dependencies(apps matrixcompletion-sgd-cpu)
target_link_libraries(matrixcompletion-sgd-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small1 matrixcompletion-sgd-cpu INPUT Epinions_dataset INPUT_URI "${RDG_EPINIONS}" --edgePropertyName=value --algo=sgdByItems NO_VERIFY)

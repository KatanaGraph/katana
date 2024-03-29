add_executable(matrixcompletion-cpu matrixCompletion.cpp)
add_dependencies(apps matrixcompletion-cpu)
target_link_libraries(matrixcompletion-cpu PRIVATE Katana::graph lonestar)

if(CMAKE_COMPILER_IS_GNUCC)
  target_compile_options(matrixcompletion-cpu PRIVATE -ffast-math)
endif()

find_package(Eigen3 CONFIG)
if(Eigen_FOUND)
  target_link_libraries(matrixcompletion-cpu Eigen3::Eigen)
  target_compile_definitions(matrixcompletion-cpu PRIVATE -DHAS_EIGEN -DEIGEN_DONT_PARALLELIZE)
endif()

if (Eigen_FOUND)
  add_test_scale(small-sync matrixcompletion-cpu -algo=syncALS -lambda=0.001 -learningRate=0.01 -learningRateFunction=intel -tolerance=0.01 -useSameLatentVector -useDetInit "${MISC_TEST_DATASETS}/weighted/bipartite/Epinions_dataset.gr")

  add_test_scale(small-simple matrixcompletion-cpu -algo=simpleALS -lambda=0.001 -learningRate=0.01 -learningRateFunction=intel -tolerance=0.01 -useSameLatentVector -useDetInit "${MISC_TEST_DATASETS}/weighted/bipartite/Epinions_dataset.gr")
endif()

add_test_scale(small-edge matrixcompletion-cpu INPUT Epinions_dataset INPUT_URI "${MISC_TEST_DATASETS}/weighted/bipartite/Epinions_dataset.gr" NOT_QUICK NO_VERIFY -algo=sgdBlockEdge -lambda=0.001 -learningRate=0.01 -learningRateFunction=intel -tolerance=0.01 -useSameLatentVector -useDetInit)

add_test_scale(small-jump matrixcompletion-cpu INPUT Epinions_dataset INPUT_URI "${MISC_TEST_DATASETS}/weighted/bipartite/Epinions_dataset.gr"  NOT_QUICK NO_VERIFY -algo=sgdBlockJump -lambda=0.001 -learningRate=0.01 -learningRateFunction=intel -tolerance=0.01 -useSameLatentVector -useDetInit)

add_test_scale(small-byitems matrixcompletion-cpu INPUT Epinions_dataset INPUT_URI "${MISC_TEST_DATASETS}/weighted/bipartite/Epinions_dataset.gr"  NOT_QUICK NO_VERIFY -algo=sgdByItems -lambda=0.001 -learningRate=0.01 -learningRateFunction=intel -tolerance=0.01 -useSameLatentVector -useDetInit)

add_test_scale(small-byedges matrixcompletion-cpu INPUT Epinions_dataset INPUT_URI "${MISC_TEST_DATASETS}/weighted/bipartite/Epinions_dataset.gr"  NOT_QUICK NO_VERIFY -algo=sgdByEdges -lambda=0.001 -learningRate=0.01 -learningRateFunction=intel -tolerance=0.01 -useSameLatentVector -useDetInit)

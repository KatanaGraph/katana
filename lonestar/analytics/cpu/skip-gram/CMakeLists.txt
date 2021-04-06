add_executable(skip-gram-cpu Embedding.cpp NeuralNetwork/SkipGramModelTrainer.cpp  Huffman/HuffmanCoding.cpp)
add_dependencies(apps skip-gram-cpu)
target_link_libraries(skip-gram-cpu PRIVATE Katana::galois lonestar)
install(TARGETS skip-gram-cpu DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT apps EXCLUDE_FROM_ALL)

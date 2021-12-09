if(KATANA_USE_GPU)
  add_executable(cuda_test cuda_test.cu)
  target_link_libraries(cuda_test PUBLIC katana_support katana_galois katana_graph)
endif()

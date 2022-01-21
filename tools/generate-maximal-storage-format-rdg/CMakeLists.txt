add_executable(generate-maximal-storage-format-rdg generate-maximal-storage-format-rdg.cpp)
target_link_libraries(generate-maximal-storage-format-rdg katana_graph LLVMSupport)
install(TARGETS generate-maximal-storage-format-rdg
  COMPONENT tools
  )

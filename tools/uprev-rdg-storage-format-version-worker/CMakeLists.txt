add_executable(uprev-rdg-storage-format-version-worker uprev-rdg-storage-format-version-worker.cpp)
target_link_libraries(uprev-rdg-storage-format-version-worker katana_galois LLVMSupport)
install(TARGETS uprev-rdg-storage-format-version-worker
  COMPONENT tools
  )

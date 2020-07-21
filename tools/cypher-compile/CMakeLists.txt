add_executable(cypher-compile cypher-compile.cpp)
target_link_libraries(cypher-compile galois_query LLVMSupport)
install(TARGETS cypher-compile
  EXPORT GaloisTargets
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  COMPONENT tools
)

add_subdirectory(test)

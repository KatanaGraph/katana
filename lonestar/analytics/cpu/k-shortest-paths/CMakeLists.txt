add_executable(k-shortest-paths-cpu SSSP.cpp)
add_dependencies(apps k-shortest-paths-cpu)
<<<<<<< HEAD
target_link_libraries(k-shortest-paths-cpu PRIVATE Katana::galois lonestar)
=======
target_link_libraries(k-shortest-paths-cpu PRIVATE Galois::shmem lonestar)
>>>>>>> 5932d41058bf495c68c0bcde5fa8b09686a2985b
install(TARGETS k-shortest-paths-cpu DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT apps EXCLUDE_FROM_ALL)

add_test_scale(small1 k-shortest-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" -delta=8 --edgePropertyName=value)

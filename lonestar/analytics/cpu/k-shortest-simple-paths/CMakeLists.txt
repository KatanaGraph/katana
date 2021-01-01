add_executable(k-shortest-simple-paths-cpu yen_k_SSSP.cpp)
add_dependencies(apps k-shortest-simple-paths-cpu)
target_link_libraries(k-shortest-simple-paths-cpu PRIVATE Katana::galois lonestar)
install(TARGETS k-shortest-simple-paths-cpu DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT apps EXCLUDE_FROM_ALL)

add_test_scale(small1 k-shortest-simple-paths-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" -delta=8 --edgePropertyName=value)

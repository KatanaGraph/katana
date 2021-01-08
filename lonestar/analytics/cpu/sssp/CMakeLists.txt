add_executable(sssp-cpu sssp_cli.cpp)
add_dependencies(apps sssp-cpu)
target_link_libraries(sssp-cpu PRIVATE Galois::shmem lonestar)
install(TARGETS sssp-cpu DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT apps EXCLUDE_FROM_ALL)

add_test_scale(small1 sssp-cpu INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15" -delta=8 --edgePropertyName=value --algo=Automatic)
#add_test_scale(small2 sssp-cpu "${BASEINPUT}/propertygraphs/rmat15" -delta=8 --edgePropertyName=value)

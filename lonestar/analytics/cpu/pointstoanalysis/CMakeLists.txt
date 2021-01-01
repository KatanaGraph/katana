add_executable(pointstoanalysis-cpu PointsTo.cpp)
add_dependencies(apps pointstoanalysis-cpu)
target_link_libraries(pointstoanalysis-cpu PRIVATE Katana::galois lonestar)
install(TARGETS pointstoanalysis-cpu DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT apps EXCLUDE_FROM_ALL)

add_test_scale(small pointstoanalysis-cpu INPUT gap_constraints INPUT_URI "${BASEINPUT}/java/pta/gap_constraints.txt" NO_VERIFY)

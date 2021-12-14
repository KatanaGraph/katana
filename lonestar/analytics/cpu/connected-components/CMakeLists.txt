add_executable(connected-components-cpu connected_components_cli.cpp)
add_dependencies(apps connected-components-cpu)
target_link_libraries(connected-components-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_symmetric" "-symmetricGraph" "-algo=LabelProp")

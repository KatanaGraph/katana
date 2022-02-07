add_executable(connected-components-cpu connected_components_cli.cpp)
add_dependencies(apps connected-components-cpu)
target_link_libraries(connected-components-cpu PRIVATE Katana::graph lonestar)

add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15_SYMMETRIC}" "-symmetricGraph" "-algo=LabelProp")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15_SYMMETRIC}" "-symmetricGraph" "-algo=Synchronous")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15_SYMMETRIC}" "-symmetricGraph" "-algo=Asynchronous")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15_SYMMETRIC}" "-symmetricGraph" "-algo=Afforest")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15_SYMMETRIC}" "-symmetricGraph" "-algo=EdgeTiledAfforest")

add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" "-algo=LabelProp")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" "-algo=Synchronous")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" "-algo=Asynchronous")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" "-algo=Afforest")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT rmat15 INPUT_URI "${RDG_RMAT15}" "-algo=EdgeTiledAfforest")

## Test TranformView
add_test_scale(small connected-components-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" "--node_types=Person" "-algo=EdgeTiledAfforest")
add_test_scale(small connected-components-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" "--edge_types=CONTAINER_OF" "-algo=EdgeTiledAfforest") 

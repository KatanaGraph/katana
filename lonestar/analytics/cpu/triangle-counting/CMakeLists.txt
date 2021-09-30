add_executable(triangle-counting-cpu triangle_counting_cli.cpp)
add_dependencies(apps triangle-counting-cpu)
target_link_libraries(triangle-counting-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small-ordered-relabel triangle-counting-cpu INPUT rmat15_cleaned_symmetric INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NO_CI NO_VERIFY -symmetricGraph -algo=orderedCount --relabel=true)
add_test_scale(small-ordered triangle-counting-cpu  INPUT rmat15_cleaned_symmetric INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NO_CI NO_VERIFY -symmetricGraph -algo=orderedCount)
add_test_scale(small-node triangle-counting-cpu  INPUT rmat15_cleaned_symmetric INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NO_CI NO_VERIFY  -symmetricGraph -algo=nodeiterator)
add_test_scale(small-edge triangle-counting-cpu  INPUT rmat15_cleaned_symmetric INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NO_CI NO_VERIFY -symmetricGraph -algo=edgeiterator)

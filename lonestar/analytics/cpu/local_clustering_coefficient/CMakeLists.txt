add_executable(local-clustering-coefficient-cpu local_clustering_coefficient_cli.cpp)
add_dependencies(apps local-clustering-coefficient-cpu)
target_link_libraries(local-clustering-coefficient-cpu PRIVATE Katana::galois lonestar)
install(TARGETS local-clustering-coefficient-cpu DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT apps EXCLUDE_FROM_ALL)

add_test_scale(small-ordered-atomics-relabel local-clustering-coefficient-cpu INPUT rmat15_cleaned_symmetric INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NOT_QUICK NO_VERIFY -symmetricGraph -algo=orderedCountAtomics --relabel=true)
add_test_scale(small-ordered-atomics local-clustering-coefficient-cpu  INPUT rmat15_cleaned_symmetric INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NOT_QUICK NO_VERIFY -symmetricGraph -algo=orderedCountAtomics)

add_test_scale(small-ordered-perThread-relabel local-clustering-coefficient-cpu INPUT rmat15_cleaned_symmetric INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NOT_QUICK NO_VERIFY -symmetricGraph -algo=orderedCountPerThread --relabel=true)
add_test_scale(small-ordered-perThread local-clustering-coefficient-cpu  INPUT rmat15_cleaned_symmetric INPUT_URI "${BASEINPUT}/propertygraphs/rmat15_cleaned_symmetric" NOT_QUICK NO_VERIFY -symmetricGraph -algo=orderedCountPerThread)

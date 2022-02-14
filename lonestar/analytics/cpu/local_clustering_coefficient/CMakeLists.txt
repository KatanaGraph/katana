add_executable(local-clustering-coefficient-cpu local_clustering_coefficient_cli.cpp)
add_dependencies(apps local-clustering-coefficient-cpu)
target_link_libraries(local-clustering-coefficient-cpu PRIVATE Katana::galois lonestar)

add_test_scale(small-ordered-atomics-relabel local-clustering-coefficient-cpu INPUT rmat15_cleaned_symmetric INPUT_URI "${RDG_RMAT15_CLEANED_SYMMETRIC}" NOT_QUICK NO_VERIFY -symmetricGraph -algo=orderedCountAtomics --relabel=true)
add_test_scale(small-ordered-atomics local-clustering-coefficient-cpu  INPUT rmat15_cleaned_symmetric INPUT_URI "${RDG_RMAT15_CLEANED_SYMMETRIC}" NOT_QUICK NO_VERIFY -symmetricGraph -algo=orderedCountAtomics)

add_test_scale(small-ordered-perThread-relabel local-clustering-coefficient-cpu INPUT rmat15_cleaned_symmetric INPUT_URI "${RDG_RMAT15_CLEANED_SYMMETRIC}" NOT_QUICK NO_VERIFY -symmetricGraph -algo=orderedCountPerThread --relabel=true)
add_test_scale(small-ordered-perThread local-clustering-coefficient-cpu  INPUT rmat15_cleaned_symmetric INPUT_URI "${RDG_RMAT15_CLEANED_SYMMETRIC}" NOT_QUICK NO_VERIFY -symmetricGraph -algo=orderedCountPerThread)

## Test TranformView
## XXX TODO(gill): Can not put more than 1 views right now: Example: Putting SortedView on Undirected view.
#add_test_scale(small-ordered-perThread-relabel local-clustering-coefficient-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person)
#add_test_scale(small-ordered-perThread-relabel local-clustering-coefficient-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF)

#XXX TODO(gill): This is a workaround for the issue above; Ldbc003 is not symmetric
#add_test_scale(small-ordered-perThread-relabel local-clustering-coefficient-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --node_types=Person -symmetricGraph )
#add_test_scale(small-ordered-perThread-relabel local-clustering-coefficient-cpu NO_VERIFY INPUT ldbc003 INPUT_URI "${RDG_LDBC_003}" --edge_types=CONTAINER_OF -symmetricGraph )

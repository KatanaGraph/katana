## csv, rdg test inputs
set(KATANA_TEST_DATASETS ${CMAKE_CURRENT_LIST_DIR}/../../external/test-datasets)
set(RDG_TEST_DATASETS ${KATANA_TEST_DATASETS}/rdg_datasets)
set(CSV_TEST_DATASETS ${KATANA_TEST_DATASETS}/csv_datasets)
set(MISC_TEST_DATASETS ${KATANA_TEST_DATASETS}/misc_datasets)

## latest supported rdg storage_format_version
#TODO(emcginnis) get this envar in RDGPartHeader.h instead of having to hard code it here and there
set(KATANA_RDG_STORAGE_FORMAT_VERSION "3")


## returns path to the specified rdg dataset at the specified storage_format_version
# unless you know you need a specific storage_format_version
# you should probably just use "get_rdg_dataset" defined below
function(rdg_dataset_at_version return_path rdg_dataset_name storage_format_version)
  set(path "${RDG_TEST_DATASETS}/${rdg_dataset_name}/storage_format_version_${storage_format_version}")
  set(${return_path} ${path} PARENT_SCOPE)
endfunction()

## returns path to the specified rdg dataset at the latest supported storage_format_version
function(rdg_dataset return_path rdg_dataset_name)
  rdg_dataset_at_version(path ${rdg_dataset_name} ${KATANA_RDG_STORAGE_FORMAT_VERSION})
  set(${return_path} ${path} PARENT_SCOPE)
endfunction()

function(add_test_dataset_fixture proj_bin_dir orig_loc suffix tmp_loc fixture_group_name)
### Setup a copy of the test_dataset for use in tests to avoid writing to shared datasets
# proj_bin_dir is the tests ${PROJECT_BINARY_DIRECTORY}
# orig_loc is the location of the test dataset to make a copy of
# suffix is the unique tag to add to this fixture, usually calling test name
# tmp_loc is the copied location of the dataset, returned to the caller
# fixture_name is the unique name of the created test_dataset_fixture
## This should only be used by tests that need to write/store/commit to the input test_dataset

### Usage:
## setup the fixtures
# add_test_dataset_fixture(${PROJECT_BINARY_DIR} ${dataset_path} -${test_name} tmp_input_location input-setup-fixture-group)
## use ${tmp_input_location} in your add_test() call
## finally, assign the fixture group to your test
# set_property(TEST ${test_name}
#  APPEND PROPERTY
#  FIXTURES_REQUIRED ${input-setup-fixture-group})

# note that the value in ${fixture_group_name} is not the name of a test,
# but an arbitrary string, to which
# tests can be added to the group using FIXTURE_SETUP
# and tests can require the group using FIXTURE_REQUIRED

  set(tmp_dataset_location ${proj_bin_dir}/Testing/Temporary/input-dataset${suffix})
  add_test(NAME clean-dataset${suffix}
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${tmp_dataset_location})
  add_test(NAME setup-dataset${suffix}
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${orig_loc} ${tmp_dataset_location})

  set(group setup-dataset${suffix}-fixtures)

  set_tests_properties(clean-dataset${suffix}
    PROPERTIES
    FIXTURES_SETUP ${group})

  set_tests_properties(setup-dataset${suffix}
    PROPERTIES
    DEPENDS clean-dataset${suffix}
    FIXTURES_SETUP ${group})

  set(${tmp_loc} ${tmp_dataset_location} PARENT_SCOPE)
  set(${fixture_group_name} ${group} PARENT_SCOPE)
endfunction()


## latest storage_format_version RDG dataset paths
rdg_dataset(RDG_LDBC_003 "ldbc_003")
rdg_dataset(RDG_RMAT10 "rmat10")
rdg_dataset(RDG_RMAT15 "rmat15")
rdg_dataset(RDG_RMAT10_SYMMETRIC "rmat10_symmetric")
rdg_dataset(RDG_RMAT15_SYMMETRIC "rmat15_symmetric")
rdg_dataset(RDG_RMAT15_CLEANED_SYMMETRIC "rmat15_cleaned_symmetric")
rdg_dataset(RDG_EPINIONS "Epinions")
rdg_dataset(RDG_GNN_TESTER "gnn_tester")
rdg_dataset(RDG_GNN_K5_PART "gnn_k5_part")
rdg_dataset(RDG_GNN_K5_SINGLE "gnn_k5_single")

## Specific storage_format_version RDG dataset paths
# Don't use these unless you know you need a specific storage_format_version RDG
# Used the latest storage_format_version RDGs defined above instead
rdg_dataset_at_version(RDG_LDBC_003_V1 "ldbc_003" "1")
rdg_dataset_at_version(RDG_LDBC_003_V3 "ldbc_003" "3")

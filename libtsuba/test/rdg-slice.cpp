#include "katana/Result.h"
#include "tsuba/RDGManifest.h"
#include "tsuba/RDGSlice.h"

namespace {
// This test tests the following:
// 1. loading/unloading properties and checking the schema work (don't crash)
// 2. loading/unloading properties does not modify the schemas
// 3. loading/unloading properties changes the properties tables as expected
// 4. loading/unloading non-existent properties behaves as expected
katana::Result<void>
TestPropertyLoading(const std::string& path_to_manifest) {
  tsuba::RDGManifest manifest =
      KATANA_CHECKED(tsuba::FindManifest(path_to_manifest));
  tsuba::RDGHandle rdg_handle =
      KATANA_CHECKED(tsuba::Open(std::move(manifest), tsuba::kReadOnly));
  // RDGFile will close the handle on destroy
  tsuba::RDGFile handle(rdg_handle);

  // this arg doesn't load any useful topology, but we are only testing property
  // loading and unloading, so this should be fine
  tsuba::RDGSlice::SliceArg slice_arg{
      .node_range = std::make_pair(0, 1),
      .edge_range = std::make_pair(0, 1),
      .topo_off = 0,
      .topo_size = 0};

  std::vector<std::string> no_props;
  auto rdg_slice = KATANA_CHECKED(
      tsuba::RDGSlice::Make(rdg_handle, slice_arg, 0, no_props, no_props));

  // input is ldbc 003
  int64_t expected_num_node_props = 17;
  int64_t expected_num_edge_props = 3;
  KATANA_LOG_ASSERT(
      rdg_slice.full_node_schema()->num_fields() == expected_num_node_props);
  KATANA_LOG_ASSERT(rdg_slice.node_properties()->num_columns() == 0);
  KATANA_LOG_ASSERT(
      rdg_slice.full_edge_schema()->num_fields() == expected_num_edge_props);
  KATANA_LOG_ASSERT(rdg_slice.edge_properties()->num_columns() == 0);

  // load all properties
  // NB: in this section and the next, we re-construct the full schema from
  // scratch in every iteration of the loop - this is an implicit test that
  // loading and unloading properties does not change the full schema
  for (int64_t i = 0; i < expected_num_node_props; ++i) {
    KATANA_CHECKED(rdg_slice.load_node_property(
        rdg_slice.full_node_schema()->field(i)->name()));
  }
  for (int64_t i = 0; i < expected_num_edge_props; ++i) {
    KATANA_CHECKED(rdg_slice.load_edge_property(
        rdg_slice.full_edge_schema()->field(i)->name()));
  }

  KATANA_LOG_ASSERT(
      rdg_slice.full_node_schema()->num_fields() == expected_num_node_props);
  KATANA_LOG_ASSERT(
      rdg_slice.node_properties()->num_columns() == expected_num_node_props);
  KATANA_LOG_ASSERT(
      rdg_slice.full_edge_schema()->num_fields() == expected_num_edge_props);
  KATANA_LOG_ASSERT(
      rdg_slice.edge_properties()->num_columns() == expected_num_edge_props);

  // unload all but two properties
  for (int64_t i = 0; i < expected_num_node_props - 2; ++i) {
    KATANA_CHECKED(rdg_slice.unload_node_property(
        rdg_slice.full_node_schema()->field(i)->name()));
  }
  for (int64_t i = 0; i < expected_num_edge_props - 2; ++i) {
    KATANA_CHECKED(rdg_slice.unload_edge_property(
        rdg_slice.full_edge_schema()->field(i)->name()));
  }

  KATANA_LOG_ASSERT(
      rdg_slice.full_node_schema()->num_fields() == expected_num_node_props);
  KATANA_LOG_ASSERT(rdg_slice.node_properties()->num_columns() == 2);
  KATANA_LOG_ASSERT(
      rdg_slice.full_edge_schema()->num_fields() == expected_num_edge_props);
  KATANA_LOG_ASSERT(rdg_slice.edge_properties()->num_columns() == 2);

  // load and unload some non-existent properties
  auto res = rdg_slice.load_node_property("does not exist");
  KATANA_LOG_ASSERT(!res && res.error() == tsuba::ErrorCode::PropertyNotFound);
  res = rdg_slice.unload_node_property("does not exist");
  KATANA_LOG_ASSERT(!res && res.error() == tsuba::ErrorCode::PropertyNotFound);
  res = rdg_slice.load_edge_property("does not exist");
  KATANA_LOG_ASSERT(!res && res.error() == tsuba::ErrorCode::PropertyNotFound);
  res = rdg_slice.unload_edge_property("does not exist");
  KATANA_LOG_ASSERT(!res && res.error() == tsuba::ErrorCode::PropertyNotFound);

  KATANA_LOG_ASSERT(
      rdg_slice.full_node_schema()->num_fields() == expected_num_node_props);
  KATANA_LOG_ASSERT(rdg_slice.node_properties()->num_columns() == 2);
  KATANA_LOG_ASSERT(
      rdg_slice.full_edge_schema()->num_fields() == expected_num_edge_props);
  KATANA_LOG_ASSERT(rdg_slice.edge_properties()->num_columns() == 2);

  return katana::ResultSuccess();
}

katana::Result<void>
TestAll(const std::string& path_to_manifest) {
  KATANA_CHECKED(TestPropertyLoading(path_to_manifest));
  return katana::ResultSuccess();
}
}  // namespace

int
main(int argc, char* argv[]) {
  KATANA_LOG_ASSERT(tsuba::Init());

  if (argc <= 1) {
    KATANA_LOG_FATAL("rdg-part-header <ldbc003 prefix>");
  }

  std::string arg(argv[1]);
  if (arg.find("ldbc") == std::string::npos) {
    KATANA_LOG_FATAL("input must be a rmat15 part file");
  }

  auto test_res = TestAll(argv[1]);
  if (!test_res) {
    KATANA_LOG_FATAL("{}", test_res.error());
  }

  KATANA_LOG_ASSERT(tsuba::Fini());
  return 0;
}

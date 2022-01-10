#include "RDGPartHeader.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/tsuba.h"

// This test exercises the prop info lists in RDGPartHeader. The three primary
// operations that RDGPartHeader exposes for those lists are:
// 1. Upsert
// 2. Remove
// 3. Find
//
// This test tests all three and it also tests the PropStorageInfo state machine
// as a side-effect of testing that the find functionality can be used to
// transition PropStorageInfos stored within a RDGPartHeader.
katana::Result<void>
TestPropInfoLists(const std::string& path_to_header) {
  katana::Uri path_to_header_uri =
      KATANA_CHECKED(katana::Uri::Make(path_to_header));
  katana::RDGPartHeader under_test =
      KATANA_CHECKED(katana::RDGPartHeader::Make(path_to_header_uri));

  // ---- initial state ----
  // input is rmat15, so no node properties and one edge property
  KATANA_LOG_ASSERT(under_test.node_prop_info_list().empty());
  KATANA_LOG_ASSERT(under_test.edge_prop_info_list().size() == 1);
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("value"));
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("value")->IsAbsent());
  KATANA_LOG_ASSERT(!under_test.find_edge_prop_info("not value"));
  KATANA_LOG_ASSERT(under_test.part_prop_info_list().empty());

  // ---- upserts ----
  // insert
  katana::PropStorageInfo new_edge_prop(
      "not value", arrow::fixed_size_binary(8));
  under_test.UpsertEdgePropStorageInfo(std::move(new_edge_prop));

  // update
  katana::PropStorageInfo updated_edge_prop("value", arrow::large_binary());
  KATANA_LOG_ASSERT(updated_edge_prop.IsDirty());
  under_test.UpsertEdgePropStorageInfo(std::move(updated_edge_prop));

  KATANA_LOG_ASSERT(under_test.node_prop_info_list().empty());
  KATANA_LOG_ASSERT(under_test.edge_prop_info_list().size() == 2);
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("value"));
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("value")->IsDirty());
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("not value"));
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("not value")->IsDirty());
  KATANA_LOG_ASSERT(under_test.part_prop_info_list().empty());

  // ---- churn state machine ----
  under_test.find_edge_prop_info("value")->WasWritten("/tmp/did/not/write");
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("value")->IsClean());
  under_test.find_edge_prop_info("value")->WasUnloaded();
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("value")->IsAbsent());
  under_test.find_edge_prop_info("value")->WasLoaded(arrow::date64());
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("value")->IsClean());
  under_test.find_edge_prop_info("value")->WasUnloaded();
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("value")->IsAbsent());

  under_test.find_edge_prop_info("not value")->WasModified(arrow::date32());
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("not value")->IsDirty());
  under_test.find_edge_prop_info("not value")->WasWritten("/tmp/did/not/write");
  KATANA_LOG_ASSERT(under_test.find_edge_prop_info("not value")->IsClean());

  // ---- remove everything ----
  KATANA_CHECKED(under_test.RemoveEdgeProperty("not value"));
  under_test.RemoveEdgeProperty(0);

  KATANA_LOG_ASSERT(under_test.node_prop_info_list().empty());
  KATANA_LOG_ASSERT(under_test.edge_prop_info_list().empty());
  KATANA_LOG_ASSERT(!under_test.find_edge_prop_info("value"));
  KATANA_LOG_ASSERT(!under_test.find_edge_prop_info("not value"));
  KATANA_LOG_ASSERT(under_test.part_prop_info_list().empty());

  return katana::ResultSuccess();
}

katana::Result<void>
TestAll(const std::string& path_to_header) {
  KATANA_CHECKED(TestPropInfoLists(path_to_header));
  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  KATANA_LOG_ASSERT(katana::InitTsuba());

  if (argc <= 1) {
    KATANA_LOG_FATAL("rdg-part-header <rmat15 prefix>");
  }

  std::string arg(argv[1]);
  if (arg.find("rmat15") == std::string::npos) {
    KATANA_LOG_FATAL("input must be a rmat15 part file");
  }

  auto test_res = TestAll(argv[1]);
  if (!test_res) {
    KATANA_LOG_FATAL("{}", test_res.error());
  }

  KATANA_LOG_ASSERT(katana::FiniTsuba());
  return 0;
}

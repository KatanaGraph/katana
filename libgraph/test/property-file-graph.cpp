#include <arrow/api.h>
#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/URI.h"

namespace {

namespace fs = boost::filesystem;
std::string command_line;

template <typename T>
std::shared_ptr<arrow::Table>
MakeProps(const std::string& name, size_t size) {
  katana::TableBuilder builder{size};

  katana::ColumnOptions options;
  options.name = name;
  options.ascending_values = true;
  builder.AddColumn<T>(options);
  return builder.Finish();
}

void
TestTypesFromPropertiesCompareTypesFromStorage() {
  /*
  Scenario 1:
  1. create a graph in memory with a couple of uint8 properties
  2. Construct types from properties
  3. Commit to storage
  4. Load the graph and compare the type info from step 2 above
  */
  constexpr size_t test_length = 10;
  using PropertyType = uint8_t;
  using ThrowAwayType = int64_t;
  katana::TxnContext txn_ctx;

  RandomPolicy policy{1};
  auto g = MakeFileGraph<uint32_t>(test_length, 0, &policy, &txn_ctx);

  std::shared_ptr<arrow::Table> node_throw_away =
      MakeProps<ThrowAwayType>("node-throw-away", test_length);

  auto add_throw_away_node_result =
      g->AddNodeProperties(node_throw_away, &txn_ctx);
  KATANA_LOG_ASSERT(add_throw_away_node_result);

  std::shared_ptr<arrow::Table> edge_throw_away =
      MakeProps<ThrowAwayType>("edge-throw-away", test_length);

  auto add_throw_away_edge_result =
      g->AddNodeProperties(edge_throw_away, &txn_ctx);
  KATANA_LOG_ASSERT(add_throw_away_edge_result);

  std::shared_ptr<arrow::Table> node_props =
      MakeProps<PropertyType>("node-name", test_length);

  auto add_node_result = g->AddNodeProperties(node_props, &txn_ctx);
  KATANA_LOG_ASSERT(add_node_result);

  std::shared_ptr<arrow::Table> edge_props =
      MakeProps<PropertyType>("edge-name", test_length);

  auto add_edge_result = g->AddEdgeProperties(edge_props, &txn_ctx);
  KATANA_LOG_ASSERT(add_edge_result);

  /// Construct types from IDs.
  auto type_construction_result = g->ConstructEntityTypeIDs(&txn_ctx);
  KATANA_LOG_ASSERT(type_construction_result);

  auto uri_res = katana::URI::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  auto rdg_dir = uri_res.value();

  KATANA_LOG_VASSERT(
      g->GetNodeEntityTypeID("node-name") != katana::kUnknownEntityType,
      "Node Entity Type ID {} is of an kUnknownEntityType.",
      g->GetNodeEntityTypeID("node-name"));

  KATANA_LOG_VASSERT(
      g->GetEdgeEntityTypeID("edge-name") != katana::kUnknownEntityType,
      "Edge Entity Type ID {} is of an kUnknownEntityType.",
      g->GetEdgeEntityTypeID("edge-name"));

  KATANA_LOG_VASSERT(
      g->GetNumNodeEntityTypes() == 2, "found {} node entity types.",
      g->GetNumNodeEntityTypes());

  KATANA_LOG_VASSERT(
      g->GetNumEdgeEntityTypes() == 2, "found {} edge entity types.",
      g->GetNumEdgeEntityTypes());

  auto write_result = g->Write(rdg_dir, command_line, &txn_ctx);

  KATANA_LOG_WARN("creating temp file {}", rdg_dir);

  if (!write_result) {
    fs::remove_all(rdg_dir.path());
    KATANA_LOG_FATAL("writing result: {}", write_result.error());
  }

  katana::Result<std::unique_ptr<katana::PropertyGraph>> make_result =
      katana::PropertyGraph::Make(rdg_dir, &txn_ctx, katana::RDGLoadOptions());
  fs::remove_all(rdg_dir.path());
  if (!make_result) {
    KATANA_LOG_FATAL("making result: {}", make_result.error());
  }

  auto g2 = std::move(make_result.value());

  KATANA_LOG_VASSERT(
      g2->GetNumNodeEntityTypes() == 2, "found {} entity types.",
      g2->GetNumNodeEntityTypes());
  KATANA_LOG_ASSERT(g2->GetNumEdgeEntityTypes() == 2);

  KATANA_LOG_ASSERT((g->NumNodes()) == test_length);
  KATANA_LOG_ASSERT((g->NumEdges()) == test_length);
  KATANA_LOG_ASSERT((g2->NumNodes()) == test_length);
  KATANA_LOG_ASSERT((g2->NumEdges()) == test_length);

  KATANA_LOG_ASSERT(g->Equals(g2.get()));
}

void
TestCompositeTypesFromPropertiesCompareCompositeTypesFromStorage() {
  /*
  Scenario 2:
  1. create a graph in memory with a couple of uint8 properties
  2. Construct composite types from properties
  3. Commit to storage
  4. Load the graph and compare the type info from step 2 above
  */
  constexpr size_t test_length = 10;
  using PropertyType = uint8_t;
  using ThrowAwayType = int64_t;
  katana::TxnContext txn_ctx;

  RandomPolicy policy{1};
  auto g = MakeFileGraph<uint32_t>(test_length, 0, &policy, &txn_ctx);

  std::shared_ptr<arrow::Table> node_throw_away =
      MakeProps<ThrowAwayType>("node-throw-away", test_length);

  auto add_throw_away_node_result =
      g->AddNodeProperties(node_throw_away, &txn_ctx);
  KATANA_LOG_ASSERT(add_throw_away_node_result);

  std::shared_ptr<arrow::Table> edge_throw_away =
      MakeProps<ThrowAwayType>("edge-throw-away", test_length);

  auto add_throw_away_edge_result =
      g->AddNodeProperties(edge_throw_away, &txn_ctx);
  KATANA_LOG_ASSERT(add_throw_away_edge_result);

  std::shared_ptr<arrow::Table> node_props_one =
      MakeProps<PropertyType>("node-name-1", test_length);

  auto add_node_one_result = g->AddNodeProperties(node_props_one, &txn_ctx);
  KATANA_LOG_ASSERT(add_node_one_result);

  std::shared_ptr<arrow::Table> edge_props_one =
      MakeProps<PropertyType>("edge-name-1", test_length);

  auto add_edge_one_result = g->AddEdgeProperties(edge_props_one, &txn_ctx);
  KATANA_LOG_ASSERT(add_edge_one_result);

  std::shared_ptr<arrow::Table> node_props_two =
      MakeProps<PropertyType>("node-name-2", test_length);

  auto add_node_two_result = g->AddNodeProperties(node_props_two, &txn_ctx);
  KATANA_LOG_ASSERT(add_node_two_result);

  std::shared_ptr<arrow::Table> edge_props_two =
      MakeProps<PropertyType>("edge-name-2", test_length);

  auto add_edge_two_result = g->AddEdgeProperties(edge_props_two, &txn_ctx);
  KATANA_LOG_ASSERT(add_edge_two_result);

  /// Construct types from IDs.
  auto type_construction_result = g->ConstructEntityTypeIDs(&txn_ctx);
  KATANA_LOG_ASSERT(type_construction_result);

  auto uri_res = katana::URI::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  auto rdg_dir = uri_res.value();

  KATANA_LOG_VASSERT(
      g->GetNodeEntityTypeID("node-name-1") != katana::kUnknownEntityType,
      "Node Entity Type ID {} is of an kUnknownEntityType.",
      g->GetNodeEntityTypeID("node-name-1"));

  KATANA_LOG_VASSERT(
      g->GetEdgeEntityTypeID("edge-name-1") != katana::kUnknownEntityType,
      "Edge Entity Type ID {} is of an kUnknownEntityType.",
      g->GetEdgeEntityTypeID("edge-name-1"));

  KATANA_LOG_VASSERT(
      g->GetNodeEntityTypeID("node-name-2") != katana::kUnknownEntityType,
      "Node Entity Type ID {} is of an kUnknownEntityType.",
      g->GetNodeEntityTypeID("node-name-2"));

  KATANA_LOG_VASSERT(
      g->GetEdgeEntityTypeID("edge-name-2") != katana::kUnknownEntityType,
      "Edge Entity Type ID {} is of an kUnknownEntityType.",
      g->GetEdgeEntityTypeID("edge-name-2"));

  KATANA_LOG_VASSERT(
      g->GetNumNodeEntityTypes() == 4, "found {} entity types.",
      g->GetNumNodeEntityTypes());
  KATANA_LOG_ASSERT(g->GetNumEdgeEntityTypes() == 4);

  auto write_result = g->Write(rdg_dir, command_line, &txn_ctx);

  KATANA_LOG_WARN("creating temp file {}", rdg_dir);

  if (!write_result) {
    fs::remove_all(rdg_dir.path());
    KATANA_LOG_FATAL("writing result: {}", write_result.error());
  }

  katana::Result<std::unique_ptr<katana::PropertyGraph>> make_result =
      katana::PropertyGraph::Make(rdg_dir, &txn_ctx, katana::RDGLoadOptions());
  fs::remove_all(rdg_dir.path());
  if (!make_result) {
    KATANA_LOG_FATAL("making result: {}", make_result.error());
  }

  auto g2 = std::move(make_result.value());

  KATANA_LOG_VASSERT(
      g2->GetNumNodeEntityTypes() == 4, "found {} entity types.",
      g2->GetNumNodeEntityTypes());
  KATANA_LOG_ASSERT(g2->GetNumEdgeEntityTypes() == 4);

  KATANA_LOG_ASSERT((g->NumNodes()) == test_length);
  KATANA_LOG_ASSERT((g->NumEdges()) == test_length);
  KATANA_LOG_ASSERT((g2->NumNodes()) == test_length);
  KATANA_LOG_ASSERT((g2->NumEdges()) == test_length);

  KATANA_LOG_ASSERT(g->Equals(g2.get()));
}

void
TestRoundTrip() {
  constexpr size_t test_length = 10;
  using ValueType = int32_t;
  using ThrowAwayType = int64_t;
  katana::TxnContext txn_ctx;

  RandomPolicy policy{1};
  auto g = MakeFileGraph<uint32_t>(test_length, 0, &policy, &txn_ctx);

  std::shared_ptr<arrow::Table> node_throw_away =
      MakeProps<ThrowAwayType>("node-throw-away", test_length);

  auto add_throw_away_node_result =
      g->AddNodeProperties(node_throw_away, &txn_ctx);
  KATANA_LOG_ASSERT(add_throw_away_node_result);

  std::shared_ptr<arrow::Table> edge_throw_away_props =
      MakeProps<ThrowAwayType>("edge-throw-away", test_length);

  auto add_edge_throw_away_result =
      g->AddEdgeProperties(edge_throw_away_props, &txn_ctx);
  KATANA_LOG_ASSERT(add_edge_throw_away_result);

  std::shared_ptr<arrow::Table> node_props =
      MakeProps<ValueType>("node-name", test_length);

  auto add_node_result = g->AddNodeProperties(node_props, &txn_ctx);
  KATANA_LOG_ASSERT(add_node_result);

  std::shared_ptr<arrow::Table> edge_props =
      MakeProps<ValueType>("edge-name", test_length);

  auto add_edge_result = g->AddEdgeProperties(edge_props, &txn_ctx);
  KATANA_LOG_ASSERT(add_edge_result);

  auto uri_res = katana::URI::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  auto rdg_dir = uri_res.value();

  // don't persist throwaway properties
  auto remove_node_throw_away_res =
      g->RemoveNodeProperty("node-throw-away", &txn_ctx);
  KATANA_LOG_ASSERT(remove_node_throw_away_res);

  auto remove_edge_throw_away_res =
      g->RemoveEdgeProperty("edge-throw-away", &txn_ctx);
  KATANA_LOG_ASSERT(remove_edge_throw_away_res);

  auto write_result = g->Write(rdg_dir, command_line, &txn_ctx);

  KATANA_LOG_WARN("creating temp file {}", rdg_dir);

  if (!write_result) {
    fs::remove_all(rdg_dir.path());
    KATANA_LOG_FATAL("writing result: {}", write_result.error());
  }

  katana::Result<std::unique_ptr<katana::PropertyGraph>> make_result =
      katana::PropertyGraph::Make(rdg_dir, &txn_ctx, katana::RDGLoadOptions());
  fs::remove_all(rdg_dir.path());
  if (!make_result) {
    KATANA_LOG_FATAL("making result: {}", make_result.error());
  }

  std::unique_ptr<katana::PropertyGraph> g2 = std::move(make_result.value());

  KATANA_LOG_VASSERT(
      g2->GetNumNodeProperties() == 1, "found {} properties",
      g2->GetNumNodeProperties());
  KATANA_LOG_ASSERT(g2->GetNumEdgeProperties() == 1);

  KATANA_LOG_ASSERT(g2->loaded_edge_schema()->field(0)->name() == "edge-name");
  KATANA_LOG_ASSERT(g2->loaded_node_schema()->field(0)->name() == "node-name");

  // the throwaway type was int64; make sure we didn't alias
  KATANA_LOG_ASSERT(
      g2->loaded_edge_schema()->field(0)->type()->Equals(arrow::int32()));
  KATANA_LOG_ASSERT(
      g2->loaded_node_schema()->field(0)->type()->Equals(arrow::int32()));

  std::shared_ptr<arrow::ChunkedArray> node_property = g2->GetNodeProperty(0);
  std::shared_ptr<arrow::ChunkedArray> edge_property = g2->GetEdgeProperty(0);

  KATANA_LOG_ASSERT(
      static_cast<size_t>(node_property->length()) == test_length);
  KATANA_LOG_ASSERT(node_property->num_chunks() == 1);
  KATANA_LOG_ASSERT(
      static_cast<size_t>(edge_property->length()) == test_length);
  KATANA_LOG_ASSERT(edge_property->num_chunks() == 1);

  {
    std::shared_ptr<arrow::Array> node_chunk = node_property->chunk(0);
    std::shared_ptr<arrow::Array> edge_chunk = edge_property->chunk(0);
    auto node_data = std::static_pointer_cast<arrow::Int32Array>(node_chunk);
    auto edge_data = std::static_pointer_cast<arrow::Int32Array>(edge_chunk);

    ValueType value{};
    for (size_t i = 0; i < test_length; ++i) {
      KATANA_LOG_ASSERT(!node_data->IsNull(i) && node_data->Value(i) == value);
      KATANA_LOG_ASSERT(!edge_data->IsNull(i) && edge_data->Value(i) == value);
      ++value;
    }
  }
}

void
TestGarbageMetadata() {
  auto uri_res = katana::URI::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  auto temp_dir = uri_res.value();
  auto rdg_file = temp_dir.Join("/meta");

  std::ofstream out(rdg_file.path());
  out << "garbage to make the file non-empty";
  out.close();

  katana::TxnContext txn_ctx;
  auto no_dir_result =
      katana::PropertyGraph::Make(rdg_file, &txn_ctx, katana::RDGLoadOptions());
  fs::remove_all(temp_dir.path());
  KATANA_LOG_ASSERT(!no_dir_result.has_value());
}

katana::URI
MakePFGFile(const std::string& n1name) {
  constexpr size_t test_length = 10;
  using V0 = int32_t;
  using V1 = uint64_t;
  const std::string n0name = "n0";
  const std::string e0name = "e0";
  const std::string e1name = "e1";
  katana::TxnContext txn_ctx;

  RandomPolicy policy{1};
  auto g = MakeFileGraph<uint32_t>(test_length, 0, &policy, &txn_ctx);

  std::shared_ptr<arrow::Table> node_props = MakeProps<V0>(n0name, test_length);

  auto add_node_result = g->AddNodeProperties(node_props, &txn_ctx);
  KATANA_LOG_ASSERT(add_node_result);

  add_node_result =
      g->AddNodeProperties(MakeProps<V1>(n1name, test_length), &txn_ctx);
  if (!add_node_result) {
    return katana::URI();
  }

  std::shared_ptr<arrow::Table> edge_props = MakeProps<V0>(e0name, test_length);

  auto add_edge_result = g->AddEdgeProperties(edge_props, &txn_ctx);
  KATANA_LOG_ASSERT(add_edge_result);

  auto unique_result = katana::URI::MakeRand("/tmp/propertygraphtests");
  KATANA_LOG_ASSERT(unique_result);
  auto rdg_file = std::move(unique_result.value());

  auto write_result = g->Write(rdg_file, command_line, &txn_ctx);

  KATANA_LOG_WARN("creating temp file {}", rdg_file);

  if (!write_result) {
    fs::remove_all(rdg_file.path());
    KATANA_LOG_FATAL("writing result: {}", write_result.error());
  }
  return rdg_file;
}

void
TestSimplePGs() {
  auto rdg_file = MakePFGFile("n0");
  KATANA_LOG_ASSERT(rdg_file.empty());
  rdg_file = MakePFGFile("n1");
  katana::TxnContext txn_ctx;
  katana::Result<std::unique_ptr<katana::PropertyGraph>> make_result =
      katana::PropertyGraph::Make(rdg_file, &txn_ctx, katana::RDGLoadOptions());
  fs::remove_all(rdg_file.path());
  KATANA_LOG_ASSERT(make_result);
}

void
TestTopologyAccess() {
  RandomPolicy policy{3};
  katana::TxnContext txn_ctx;
  auto g = MakeFileGraph<uint32_t>(10, 1, &policy, &txn_ctx);

  KATANA_LOG_ASSERT(g->size() == 10);
  KATANA_LOG_ASSERT(g->NumNodes() == 10);
  KATANA_LOG_ASSERT(g->NumEdges() == 30);

  for (int i = 0; i < 10; ++i) {
    KATANA_LOG_ASSERT(
        std::distance(g->OutEdges(i).begin(), g->OutEdges(i).end()) == 3);
    KATANA_LOG_ASSERT(g->OutEdges(i).size() == 3);
    KATANA_LOG_ASSERT(g->OutEdges(i));
    KATANA_LOG_ASSERT(!g->OutEdges(i).empty());
  }
  int n_nodes = 0;
  for (katana::PropertyGraph::Node i : *g) {
    auto _ignore = g->GetNodeProperty(0)->chunk(0)->GetScalar(i);
    n_nodes++;
    int n_edges = 0;
    for (auto e : g->OutEdges(i)) {
      auto __ignore = g->GetEdgeProperty(0)->chunk(0)->GetScalar(e);
      n_edges++;
    }
    KATANA_LOG_ASSERT(n_edges == 3);
  }
  KATANA_LOG_ASSERT(n_nodes == 10);
}
}  // namespace

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;

  std::ostringstream cmdout;
  for (int i = 0; i < argc; ++i) {
    cmdout << argv[i];
    if (i != argc - 1)
      cmdout << " ";
  }
  command_line = cmdout.str();

  TestRoundTrip();
  TestGarbageMetadata();
  TestSimplePGs();
  TestTopologyAccess();
  TestTypesFromPropertiesCompareTypesFromStorage();
  TestCompositeTypesFromPropertiesCompareCompositeTypesFromStorage();

  return 0;
}

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
TestRoundTrip() {
  constexpr size_t test_length = 10;
  using ValueType = int32_t;
  using ThrowAwayType = int64_t;

  RandomPolicy policy{1};
  auto g = MakeFileGraph<uint32_t>(test_length, 0, &policy);

  std::shared_ptr<arrow::Table> node_throw_away =
      MakeProps<ThrowAwayType>("node-throw-away", test_length);

  auto add_throw_away_node_result = g->AddNodeProperties(node_throw_away);
  KATANA_LOG_ASSERT(add_throw_away_node_result);

  std::shared_ptr<arrow::Table> edge_throw_away_props =
      MakeProps<ThrowAwayType>("edge-throw-away", test_length);

  auto add_edge_throw_away_result = g->AddEdgeProperties(edge_throw_away_props);
  KATANA_LOG_ASSERT(add_edge_throw_away_result);

  std::shared_ptr<arrow::Table> node_props =
      MakeProps<ValueType>("node-name", test_length);

  auto add_node_result = g->AddNodeProperties(node_props);
  KATANA_LOG_ASSERT(add_node_result);

  std::shared_ptr<arrow::Table> edge_props =
      MakeProps<ValueType>("edge-name", test_length);

  auto add_edge_result = g->AddEdgeProperties(edge_props);
  KATANA_LOG_ASSERT(add_edge_result);

  auto uri_res = katana::Uri::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  std::string rdg_dir(uri_res.value().path());  // path() because local

  // don't persist throwaway properties
  auto remove_node_throw_away_res = g->RemoveNodeProperty("node-throw-away");
  KATANA_LOG_ASSERT(remove_node_throw_away_res);

  auto remove_edge_throw_away_res = g->RemoveEdgeProperty("edge-throw-away");
  KATANA_LOG_ASSERT(remove_edge_throw_away_res);

  auto write_result = g->Write(rdg_dir, command_line);

  KATANA_LOG_WARN("creating temp file {}", rdg_dir);

  if (!write_result) {
    fs::remove_all(rdg_dir);
    KATANA_LOG_FATAL("writing result: {}", write_result.error());
  }

  katana::Result<std::unique_ptr<katana::PropertyGraph>> make_result =
      katana::PropertyGraph::Make(rdg_dir, tsuba::RDGLoadOptions());
  fs::remove_all(rdg_dir);
  if (!make_result) {
    KATANA_LOG_FATAL("making result: {}", make_result.error());
  }

  std::unique_ptr<katana::PropertyGraph> g2 = std::move(make_result.value());

  KATANA_LOG_ASSERT(g2->GetNumNodeProperties() == 1);
  KATANA_LOG_ASSERT(g2->GetNumEdgeProperties() == 1);

  KATANA_LOG_ASSERT(g2->edge_schema()->field(0)->name() == "edge-name");
  KATANA_LOG_ASSERT(g2->node_schema()->field(0)->name() == "node-name");

  // the throwaway type was int64; make sure we didn't alias
  KATANA_LOG_ASSERT(
      g2->edge_schema()->field(0)->type()->Equals(arrow::int32()));
  KATANA_LOG_ASSERT(
      g2->node_schema()->field(0)->type()->Equals(arrow::int32()));

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
  auto uri_res = katana::Uri::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  std::string temp_dir(uri_res.value().path());  // path because local

  std::string rdg_file{temp_dir};
  rdg_file += "/meta";

  std::ofstream out(rdg_file);
  out << "garbage to make the file non-empty";
  out.close();

  auto no_dir_result =
      katana::PropertyGraph::Make(rdg_file, tsuba::RDGLoadOptions());
  fs::remove_all(temp_dir);
  KATANA_LOG_ASSERT(!no_dir_result.has_value());
}

std::string
MakePFGFile(const std::string& n1name) {
  constexpr size_t test_length = 10;
  using V0 = int32_t;
  using V1 = uint64_t;
  const std::string n0name = "n0";
  const std::string e0name = "e0";
  const std::string e1name = "e1";

  RandomPolicy policy{1};
  auto g = MakeFileGraph<uint32_t>(test_length, 0, &policy);

  std::shared_ptr<arrow::Table> node_props = MakeProps<V0>(n0name, test_length);

  auto add_node_result = g->AddNodeProperties(node_props);
  KATANA_LOG_ASSERT(add_node_result);

  add_node_result = g->AddNodeProperties(MakeProps<V1>(n1name, test_length));
  if (!add_node_result) {
    return "";
  }

  std::shared_ptr<arrow::Table> edge_props = MakeProps<V0>(e0name, test_length);

  auto add_edge_result = g->AddEdgeProperties(edge_props);
  KATANA_LOG_ASSERT(add_edge_result);

  auto unique_result = katana::Uri::MakeRand("/tmp/propertygraphtests");
  KATANA_LOG_ASSERT(unique_result);
  std::string rdg_file(
      std::move(unique_result.value().path()));  // path() for local

  auto write_result = g->Write(rdg_file, command_line);

  KATANA_LOG_WARN("creating temp file {}", rdg_file);

  if (!write_result) {
    fs::remove_all(rdg_file);
    KATANA_LOG_FATAL("writing result: {}", write_result.error());
  }
  return rdg_file;
}

void
TestSimplePGs() {
  auto rdg_file = MakePFGFile("n0");
  KATANA_LOG_ASSERT(rdg_file.empty());
  rdg_file = MakePFGFile("n1");
  katana::Result<std::unique_ptr<katana::PropertyGraph>> make_result =
      katana::PropertyGraph::Make(rdg_file, tsuba::RDGLoadOptions());
  fs::remove_all(rdg_file);
  KATANA_LOG_ASSERT(make_result);
}

void
TestTopologyAccess() {
  RandomPolicy policy{3};
  auto g = MakeFileGraph<uint32_t>(10, 1, &policy);

  KATANA_LOG_ASSERT(g->size() == 10);
  KATANA_LOG_ASSERT(g->num_nodes() == 10);
  KATANA_LOG_ASSERT(g->num_edges() == 30);

  for (int i = 0; i < 10; ++i) {
    KATANA_LOG_ASSERT(
        std::distance(g->edges(i).begin(), g->edges(i).end()) == 3);
    KATANA_LOG_ASSERT(g->edges(i).size() == 3);
    KATANA_LOG_ASSERT(g->edges(i));
    KATANA_LOG_ASSERT(!g->edges(i).empty());
  }
  int n_nodes = 0;
  for (katana::PropertyGraph::Node i : *g) {
    auto _ignore = g->GetNodeProperty(0)->chunk(0)->GetScalar(i);
    n_nodes++;
    int n_edges = 0;
    for (auto e : g->edges(i)) {
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

  return 0;
}

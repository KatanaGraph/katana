#include <arrow/api.h>
#include <boost/filesystem.hpp>

#include "TestPropertyGraph.h"
#include "galois/Logging.h"
#include "galois/Uri.h"
#include "galois/graphs/PropertyFileGraph.h"
#include "tsuba/tsuba.h"

namespace fs = boost::filesystem;
std::string command_line;

template <typename T>
std::shared_ptr<arrow::Table>
MakeTable(const std::string& name, size_t size) {
  TableBuilder builder{size};

  ColumnOptions options;
  options.name = name;
  options.ascending_values = true;
  builder.AddColumn<T>(options);
  return builder.Finish();
}

std::string
MakePFGFile(const std::string& n1name) {
  constexpr size_t test_length = 10;
  using V0 = int32_t;
  using V1 = uint64_t;
  const std::string n0name = "n0";
  const std::string e0name = "e0";
  const std::string e1name = "e1";

  auto g = std::make_unique<galois::graphs::PropertyFileGraph>();

  std::shared_ptr<arrow::Table> node_table = MakeTable<V0>(n0name, test_length);

  auto add_node_result = g->AddNodeProperties(node_table);
  GALOIS_LOG_ASSERT(add_node_result);

  auto mark_node_persistent = g->MarkNodePropertiesPersistent({n0name});
  GALOIS_LOG_ASSERT(mark_node_persistent);

  add_node_result = g->AddNodeProperties(MakeTable<V1>(n1name, test_length));
  if (!add_node_result) {
    return "";
  }

  mark_node_persistent = g->MarkNodePropertiesPersistent({n1name});
  GALOIS_LOG_ASSERT(mark_node_persistent);

  std::shared_ptr<arrow::Table> edge_table = MakeTable<V0>(e0name, test_length);

  auto add_edge_result = g->AddEdgeProperties(edge_table);
  GALOIS_LOG_ASSERT(add_edge_result);

  auto mark_edge_persistent = g->MarkEdgePropertiesPersistent({e0name});
  GALOIS_LOG_ASSERT(mark_edge_persistent);

  auto unique_result = galois::Uri::MakeRand("/tmp/propertygraphtests");
  GALOIS_LOG_ASSERT(unique_result);
  std::string rdg_file(
      std::move(unique_result.value().path()));  // path() for local

  auto write_result = g->Write(rdg_file, command_line);

  GALOIS_LOG_WARN("creating temp file {}", rdg_file);

  if (!write_result) {
    fs::remove_all(rdg_file);
    GALOIS_LOG_FATAL("writing result: {}", write_result.error());
  }
  return rdg_file;
}

void
TestSimplePGs() {
  auto rdg_file = MakePFGFile("n0");
  GALOIS_LOG_ASSERT(rdg_file.empty());
  rdg_file = MakePFGFile("n1");
  galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
      make_result = galois::graphs::PropertyFileGraph::Make(rdg_file);
  fs::remove_all(rdg_file);
  GALOIS_LOG_ASSERT(make_result);
}

int
main(int argc, char** argv) {
  if (auto res = tsuba::Init(); !res) {
    GALOIS_LOG_FATAL("libtsuba failed to init");
  }
  std::ostringstream cmdout;
  for (int i = 0; i < argc; ++i) {
    cmdout << argv[i];
    if (i != argc - 1)
      cmdout << " ";
  }
  command_line = cmdout.str();

  TestSimplePGs();

  if (auto res = tsuba::Fini(); !res) {
    GALOIS_LOG_FATAL("libtsuba failed to fini");
  }
  return 0;
}

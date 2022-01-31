#include <string.h>

#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/RDG.h"
#include "katana/SharedMemSys.h"
#include "katana/TransformationView.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> nodeTypes(
    cll::Positional, cll::desc("<node types to project>"), cll::Required);

static cll::opt<std::string> edgeTypes(
    cll::Positional, cll::desc("<edge types to project>"), cll::init(""));

struct TempNodeProp : public katana::PODProperty<uint64_t> {};
using NodeData = std::tuple<TempNodeProp>;
using EdgeData = std::tuple<>;

using Graph = katana::TypedPropertyGraph<NodeData, EdgeData>;
using GNode = typename Graph::Node;

katana::PropertyGraph
LoadGraph(const std::string& rdg_file) {
  KATANA_LOG_ASSERT(!rdg_file.empty());
  katana::TxnContext txn_ctx;
  auto g_res =
      katana::PropertyGraph::Make(rdg_file, &txn_ctx, katana::RDGLoadOptions());

  if (!g_res) {
    KATANA_LOG_FATAL("making result: {}", g_res.error());
  }
  katana::PropertyGraph g = std::move(*g_res.value());
  return g;
}

void
SplitString(std::string& str, std::vector<std::string>* vec) {
  size_t start;
  size_t end = 0;

  while ((start = str.find_first_not_of(',', end)) != std::string::npos) {
    end = str.find(',', start);
    vec->push_back(str.substr(start, end - start));
  }
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  katana::PropertyGraph full_graph = LoadGraph(inputFile);

  std::vector<std::string> node_types;
  SplitString(nodeTypes, &node_types);

  std::vector<std::string> edge_types;
  SplitString(edgeTypes, &edge_types);

  auto pg_view = katana::TransformationView::MakeProjectedGraph(
      full_graph, node_types, edge_types);

  katana::analytics::TemporaryPropertyGuard temp_node_property{
      full_graph.NodeMutablePropertyView()};

  std::vector<std::string> node_props;
  node_props.emplace_back(temp_node_property.name());
  katana::TxnContext txn_ctx;
  auto res_node_prop =
      pg_view->ConstructNodeProperties<NodeData>(&txn_ctx, node_props);

  if (!res_node_prop) {
    KATANA_LOG_FATAL(
        "Failed to Construct Properties: {}", res_node_prop.error());
  }
  auto typed_pg_view =
      Graph::Make(pg_view.get(), node_props, {}).assume_value();

  uint32_t num_valid_nodes{0};

  auto res_node_get_prop =
      full_graph.GetNodeProperty(temp_node_property.name());
  auto node_prop = res_node_get_prop.value();

  num_valid_nodes = full_graph.NumNodes() - node_prop->null_count();

  KATANA_LOG_VASSERT(
      typed_pg_view.NumNodes() > 0 &&
          full_graph.NumNodes() >= typed_pg_view.NumNodes(),
      "\n Num Nodes: {}", typed_pg_view.NumNodes());
  KATANA_LOG_VASSERT(
      typed_pg_view.NumEdges() > 0 &&
          full_graph.NumEdges() >= typed_pg_view.NumEdges(),
      "\n Num Edges: {}", typed_pg_view.NumEdges());
  KATANA_LOG_VASSERT(
      typed_pg_view.NumNodes() == num_valid_nodes,
      "\n Num Valid Nodes: {} Num Nodes: {}", num_valid_nodes,
      typed_pg_view.NumNodes());

  return 0;
}

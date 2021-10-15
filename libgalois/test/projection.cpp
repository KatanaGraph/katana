#include <string.h>

#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "tsuba/RDG.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> nodeTypes(
    cll::Positional, cll::desc("<node types to project>"), cll::Required);

static cll::opt<std::string> edgeTypes(
    cll::Positional, cll::desc("<edge types to project>"), cll::Required);

using ProjectedPropertyGraphView = katana::PropertyGraphViews::ProjectedGraph;

struct TempNodeProp : public katana::PODProperty<uint64_t> {};
using NodeData = std::tuple<TempNodeProp>;
using EdgeData = std::tuple<>;

using ProjectedGraphView = katana::TypedPropertyGraphView<
    ProjectedPropertyGraphView, NodeData, EdgeData>;
using GNode = typename ProjectedGraphView::Node;

katana::PropertyGraph
LoadGraph(const std::string& rdg_file) {
  KATANA_LOG_ASSERT(!rdg_file.empty());
  auto g_res = katana::PropertyGraph::Make(rdg_file, tsuba::RDGLoadOptions());

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

  auto pg_view =
      full_graph.BuildView<ProjectedPropertyGraphView>(node_types, edge_types);

  katana::analytics::TemporaryPropertyGuard temp_node_property{
      full_graph.NodeMutablePropertyView()};

  std::vector<std::string> node_props;
  node_props.emplace_back(temp_node_property.name());
  auto res_node_prop = katana::analytics::ConstructNodeProperties<
      ProjectedPropertyGraphView, NodeData>(&full_graph, pg_view, node_props);

  auto res_projected_graph =
      ProjectedGraphView::Make(&full_graph, pg_view, node_props, {});

  auto projected_graph = res_projected_graph.value();

  uint32_t num_valid_nodes{0};

  auto res_node_get_prop =
      full_graph.GetNodeProperty(temp_node_property.name());
  auto node_prop = res_node_get_prop.value();

  num_valid_nodes = full_graph.num_nodes() - node_prop->null_count();

  KATANA_LOG_VASSERT(
      projected_graph.num_nodes() > 0 &&
          full_graph.num_nodes() >= projected_graph.num_nodes(),
      "\n Num Nodes: {}", projected_graph.num_nodes());
  KATANA_LOG_VASSERT(
      projected_graph.num_edges() > 0 &&
          full_graph.num_edges() >= projected_graph.num_edges(),
      "\n Num Edges: {}", projected_graph.num_edges());
  KATANA_LOG_VASSERT(
      projected_graph.num_nodes() == num_valid_nodes,
      "\n Num Valid Nodes: {} Num Nodes: {}", num_valid_nodes,
      projected_graph.num_nodes());

  return 0;
}

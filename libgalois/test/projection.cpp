#include <string.h>

#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
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
using NodeData = std::tuple<>;
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

  auto projected_graph = ProjectedGraphView::Make(&full_graph, pg_view, {}, {});

  KATANA_LOG_VASSERT(
      projected_graph.value().num_nodes() > 0 &&
          full_graph.num_nodes() >= projected_graph.value().num_nodes(),
      "\n Num Nodes: {}", projected_graph.value().num_nodes());
  KATANA_LOG_VASSERT(
      projected_graph.value().num_edges() > 0 &&
          full_graph.num_edges() >= projected_graph.value().num_edges(),
      "\n Num Edges: {}", projected_graph.value().num_edges());

  return 0;
}

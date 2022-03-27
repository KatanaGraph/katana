#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/GraphTopology.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/RDG.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "storage-format-version.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

/*
 *Tests for the generation of various optional topologies on projection
 */

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> nodeTypes(
    cll::Positional, cll::desc("<node types to project>"), cll::Required);

static cll::opt<std::string> edgeTypes(
    cll::Positional, cll::desc("<edge types to project>"), cll::init(""));

void
SplitString(std::string& str, std::vector<std::string>* vec) {
  size_t start;
  size_t end = 0;

  while ((start = str.find_first_not_of(',', end)) != std::string::npos) {
    end = str.find(',', start);
    vec->push_back(str.substr(start, end - start));
  }
}

void
TestOptionalTopologyGenerationEdgeShuffleTopology(katana::PropertyGraph& pg) {
  KATANA_LOG_DEBUG("##### Testing EdgeShuffleTopology Generation #####");

  // Build a EdgeSortedByDestID view, which uses GraphTopology EdgeShuffleTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgesSortedByDestID;

  pg.BuildView<SortedGraphView>();
}

void
TestOptionalTopologyGenerationShuffleTopology(katana::PropertyGraph& pg) {
  KATANA_LOG_DEBUG("##### Testing ShuffleTopology Generation #####");

  // Build a NodesSortedByDegreeEdgesSortedByDestID view, which uses GraphTopology ShuffleTopology in the background
  using SortedGraphView =
      katana::PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID;

  pg.BuildView<SortedGraphView>();
}

void
TestOptionalTopologyGenerationEdgeTypeAwareTopology(katana::PropertyGraph& pg) {
  KATANA_LOG_DEBUG("##### Testing EdgeTypeAware Topology Generation ######");

  // Build a EdgeTypeAwareBiDir view, which uses GraphTopology EdgeTypeAwareTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgeTypeAwareBiDir;

  pg.BuildView<SortedGraphView>();
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  katana::TxnContext txn_ctx;
  auto res = katana::URI::Make(inputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", inputFile, res.error());
  }
  auto inputURI = res.value();
  katana::PropertyGraph pg = LoadGraph(inputURI);

  std::vector<std::string> node_types;
  SplitString(nodeTypes, &node_types);

  std::vector<std::string> edge_types;
  SplitString(edgeTypes, &edge_types);

  auto pg_view_res = katana::PropertyGraph::MakeProjectedGraph(
      pg, node_types.empty() ? std::nullopt : std::make_optional(node_types),
      edge_types.empty() ? std::nullopt : std::make_optional(edge_types));
  if (!pg_view_res) {
    KATANA_LOG_FATAL("Failed to construct projection: {}", pg_view_res.error());
  }
  auto pg_view = std::move(pg_view_res.value());

  TestOptionalTopologyGenerationEdgeShuffleTopology(*pg_view);
  TestOptionalTopologyGenerationShuffleTopology(*pg_view);
  TestOptionalTopologyGenerationEdgeTypeAwareTopology(*pg_view);
  return 0;
}

#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/GraphTopology.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "tsuba/RDG.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

static cll::opt<std::string> ldbc_003InputFile(
    cll::Positional, cll::desc("<ldbc_003 input file>"), cll::Required);

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
TestOptionalTopologyGenerationEdgeShuffleTopology() {
  KATANA_LOG_DEBUG("##### Testing EdgeShuffleTopology Generation #####");

  katana::PropertyGraph pg = LoadGraph(ldbc_003InputFile);

  // Build a EdgeSortedByDestID view, which uses GraphTopology EdgeShuffleTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgesSortedByDestID;

  pg.BuildView<SortedGraphView>();
}

void
TestOptionalTopologyGenerationShuffleTopology() {
  KATANA_LOG_DEBUG("##### Testing ShuffleTopology Generation #####");

  katana::PropertyGraph pg = LoadGraph(ldbc_003InputFile);

  // Build a NodesSortedByDegreeEdgesSortedByDestID view, which uses GraphTopology ShuffleTopology in the background
  using SortedGraphView =
      katana::PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID;

  pg.BuildView<SortedGraphView>();
}

void
TestOptionalTopologyGenerationEdgeTypeAwareTopology() {
  KATANA_LOG_DEBUG("##### Testing EdgeTypeAware Topology Generation ######");

  katana::PropertyGraph pg = LoadGraph(ldbc_003InputFile);

  // Build a EdgeTypeAwareBiDir view, which uses GraphTopology EdgeTypeAwareTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgeTypeAwareBiDir;

  pg.BuildView<SortedGraphView>();
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  TestOptionalTopologyGenerationEdgeShuffleTopology();
  TestOptionalTopologyGenerationShuffleTopology();
  TestOptionalTopologyGenerationEdgeTypeAwareTopology();
  return 0;
}

#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/GraphTopology.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "storage-format-version.h"
#include "tsuba/RDG.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

/*
 *Tests for the generation of various optional topologies
 */

static cll::opt<std::string> ldbc_003InputFile(
    cll::Positional, cll::desc("<ldbc_003 input file>"), cll::Required);

void
TestOptionalTopologyGenerationEdgeShuffleTopology() {
  KATANA_LOG_DEBUG("##### Testing EdgeShuffleTopology Generation #####");

  tsuba::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(ldbc_003InputFile, &txn_ctx);

  // Build a EdgeSortedByDestID view, which uses GraphTopology EdgeShuffleTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgesSortedByDestID;

  pg.BuildView<SortedGraphView>();
}

void
TestOptionalTopologyGenerationShuffleTopology() {
  KATANA_LOG_DEBUG("##### Testing ShuffleTopology Generation #####");

  tsuba::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(ldbc_003InputFile, &txn_ctx);

  // Build a NodesSortedByDegreeEdgesSortedByDestID view, which uses GraphTopology ShuffleTopology in the background
  using SortedGraphView =
      katana::PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID;

  pg.BuildView<SortedGraphView>();
}

void
TestOptionalTopologyGenerationEdgeTypeAwareTopology() {
  KATANA_LOG_DEBUG("##### Testing EdgeTypeAware Topology Generation ######");

  tsuba::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(ldbc_003InputFile, &txn_ctx);

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

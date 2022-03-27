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
 *Tests for the generation of various optional topologies
 */

static cll::opt<std::string> ldbc_003InputFile(
    cll::Positional, cll::desc("<ldbc_003 input file>"), cll::Required);

void
TestOptionalTopologyGenerationEdgeShuffleTopology(const katana::URI& rdg_dir) {
  KATANA_LOG_DEBUG("##### Testing EdgeShuffleTopology Generation #####");

  katana::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(rdg_dir);

  // Build a EdgeSortedByDestID view, which uses GraphTopology EdgeShuffleTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgesSortedByDestID;

  pg.BuildView<SortedGraphView>();
}

void
TestOptionalTopologyGenerationShuffleTopology(const katana::URI& rdg_dir) {
  KATANA_LOG_DEBUG("##### Testing ShuffleTopology Generation #####");

  katana::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(rdg_dir);

  // Build a NodesSortedByDegreeEdgesSortedByDestID view, which uses GraphTopology ShuffleTopology in the background
  using SortedGraphView =
      katana::PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID;

  pg.BuildView<SortedGraphView>();
}

void
TestOptionalTopologyGenerationEdgeTypeAwareTopology(
    const katana::URI& rdg_dir) {
  KATANA_LOG_DEBUG("##### Testing EdgeTypeAware Topology Generation ######");

  katana::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(rdg_dir);

  // Build a EdgeTypeAwareBiDir view, which uses GraphTopology EdgeTypeAwareTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgeTypeAwareBiDir;

  pg.BuildView<SortedGraphView>();
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);
  auto res = katana::URI::Make(ldbc_003InputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", ldbc_003InputFile, res.error());
  }
  auto uri = res.value();

  TestOptionalTopologyGenerationEdgeShuffleTopology(uri);
  TestOptionalTopologyGenerationShuffleTopology(uri);
  TestOptionalTopologyGenerationEdgeTypeAwareTopology(uri);
  return 0;
}

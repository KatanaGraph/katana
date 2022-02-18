#include <string>

#include <boost/filesystem.hpp>

#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/RDG.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

/* usage: ./rdg-uprev-storage-format-version.cpp <input-rdg> <output-path>
 *
 * loads the given input rdg, calls methods which create optional data structrues
 * in order to create an approximate maximal rdg for test usage
 *
 */

static cll::opt<std::string> InputFile(
    cll::Positional, cll::desc("<input rdg file>"), cll::Required);

static cll::opt<std::string> OutputFile(
    cll::Positional, cll::desc("<output rdg file>"), cll::Required);

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

std::string
StoreGraph(katana::PropertyGraph* g, std::string& output_path) {
  std::string command_line;
  katana::TxnContext txn_ctx;
  // Store graph. If there is a new storage format then storing it is enough to bump the version up.
  KATANA_LOG_WARN("writing graph at file {}", output_path);
  auto write_result = g->Write(output_path, command_line, &txn_ctx);
  if (!write_result) {
    KATANA_LOG_FATAL("writing result failed: {}", write_result.error());
  }
  return output_path;
}

void
MaximizeGraph(std::string& input_rdg, std::string& output_path) {
  katana::PropertyGraph g = LoadGraph(input_rdg);

  // Add calls which add optional data structures to the RDG here
  auto generated_sorted_view_sort1 = g.BuildView<
      katana::PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID>();
  auto generated_sorted_view_sort2 =
      g.BuildView<katana::PropertyGraphViews::EdgesSortedByDestID>();
  auto generated_sorted_view_sort3 =
      g.BuildView<katana::PropertyGraphViews::EdgeTypeAwareBiDir>();

  std::string g2_rdg_file = StoreGraph(&g, output_path);
  KATANA_LOG_WARN(
      "maximized version of {} stored at {}", input_rdg, g2_rdg_file);
}

int
main(int argc, char** argv) {
  //TODO(emcginnis): can we run this utility in distributed mode? If so, this should be distributed memory.
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  KATANA_LOG_ASSERT(!InputFile.empty());
  MaximizeGraph(InputFile, OutputFile);

  return 0;
}

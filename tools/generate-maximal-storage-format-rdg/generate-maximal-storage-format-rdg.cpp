#include <string>

#include <boost/filesystem.hpp>

#include "../../libtsuba/test/storage-format-version/v6-optional-datastructure-rdk.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/RDG.h"
#include "katana/RDKLSHIndexPrimitive.h"
#include "katana/RDKSubstructureIndexPrimitive.h"
#include "katana/Result.h"
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
LoadGraph(const katana::URI& rdg_file) {
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

katana::URI
StoreGraph(katana::PropertyGraph* g, const katana::URI& output_uri) {
  std::string command_line;
  katana::TxnContext txn_ctx;
  // Store graph. If there is a new storage format then storing it is enough to bump the version up.
  KATANA_LOG_WARN("writing graph at file {}", output_uri);

  auto write_result = g->Write(output_uri, command_line, &txn_ctx);
  if (!write_result) {
    KATANA_LOG_FATAL("writing result failed: {}", write_result.error());
  }
  return output_uri;
}

/// Load/store cycle the provided RDG to cleanly relocate the graph without
/// Carrying along stale files
katana::PropertyGraph
CleanRelocateGraphLoad(const katana::URI& rdg_file) {
  katana::PropertyGraph g_orig = LoadGraph(rdg_file);
  auto uri_res = katana::URI::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  auto tmp_rdg_dir = uri_res.value();
  auto tmp_path = StoreGraph(&g_orig, tmp_rdg_dir);

  katana::PropertyGraph g = LoadGraph(tmp_path);
  return g;
}

/// Load/store cycle the provided RDG to cleanly relocate the graph without
/// Carrying along stale files
katana::URI
CleanRelocateGraphStore(
    katana::PropertyGraph* g, const katana::URI& output_uri) {
  auto uri_res = katana::URI::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  auto tmp_rdg_dir_2 = uri_res.value();
  auto g_tmp_rdg_file = StoreGraph(g, tmp_rdg_dir_2);

  katana::PropertyGraph g_new = LoadGraph(g_tmp_rdg_file);
  auto g_new_rdg = StoreGraph(&g_new, output_uri);

  return g_new_rdg;
}

katana::Result<void>
MaximizeGraph(const katana::URI& input_uri, const katana::URI& output_uri) {
  katana::PropertyGraph g_tmp = CleanRelocateGraphLoad(input_uri);

  // Add calls which add optional data structures to the RDG here
  auto generated_sorted_view_sort1 = g_tmp.BuildView<
      katana::PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID>();
  auto generated_sorted_view_sort2 =
      g_tmp.BuildView<katana::PropertyGraphViews::EdgesSortedByDestID>();
  auto generated_sorted_view_sort3 =
      g_tmp.BuildView<katana::PropertyGraphViews::EdgeTypeAwareBiDir>();

  katana::RDKLSHIndexPrimitive lsh = GenerateLSHIndex();
  katana::RDKSubstructureIndexPrimitive substruct = GenerateSubstructIndex();

  KATANA_CHECKED(g_tmp.WriteRDKLSHIndexPrimitive(lsh));
  KATANA_CHECKED(g_tmp.WriteRDKSubstructureIndexPrimitive(substruct));

  auto g2_rdg_uri = CleanRelocateGraphStore(&g_tmp, output_uri);

  KATANA_LOG_WARN(
      "maximized version of {} stored at {}", input_uri, g2_rdg_uri);

  return katana::ResultSuccess();
}

int
main(int argc, char** argv) {
  //TODO(emcginnis): can we run this utility in distributed mode? If so, this should be distributed memory.
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  KATANA_LOG_ASSERT(!InputFile.empty());
  auto res = katana::URI::Make(InputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", InputFile, res.error());
  }
  auto InputURI = res.value();
  res = katana::URI::Make(OutputFile);
  if (!res) {
    KATANA_LOG_FATAL("output file {} error: {}", OutputFile, res.error());
  }
  auto OutputURI = res.value();

  if (auto max_res = MaximizeGraph(InputURI, OutputURI); !max_res) {
    KATANA_LOG_FATAL("failed to generate maximal graph: {}", max_res.error());
  }

  return 0;
}

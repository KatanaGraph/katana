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

static cll::opt<std::string> ldbc_003InputFile(
    cll::Positional, cll::desc("<ldbc_003 input file>"), cll::Required);

void
TestLoadGraphWithoutExternalTypes() {
  // Load existing "old" graph, which converts all uint8/bool properties into types
  // store it as a new file
  // load the new file
  // ensure the converted old graph, and the loaded new graph match
  auto g_preconvert_res =
      katana::PropertyGraph::Make(ldbc_003InputFile, tsuba::RDGLoadOptions());

  if (!g_preconvert_res) {
    KATANA_LOG_FATAL("making result: {}", g_preconvert_res.error());
  }
  std::unique_ptr<katana::PropertyGraph> g_preconvert =
      std::move(g_preconvert_res.value());

  const auto g_node_type_manager = g_preconvert->GetNodeTypeManager();

  const auto g_edge_type_manager = g_preconvert->GetEdgeTypeManager();

  auto uri_res = katana::Uri::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  std::string tmp_rdg_dir(uri_res.value().path());  // path() because local
  std::string command_line;

  // Store new converted graph
  auto write_result = g_preconvert->Write(tmp_rdg_dir, command_line);
  KATANA_LOG_WARN(
      "writing converted graph, creating temp file {}", tmp_rdg_dir);

  if (!write_result) {
    KATANA_LOG_FATAL("writing result failed: {}", write_result.error());
  }

  auto g_postconvert_res =
      katana::PropertyGraph::Make(tmp_rdg_dir, tsuba::RDGLoadOptions());
  fs::remove_all(tmp_rdg_dir);
  if (!g_postconvert_res) {
    KATANA_LOG_FATAL("making result: {}", g_postconvert_res.error());
  }
  std::unique_ptr<katana::PropertyGraph> g_postconvert =
      std::move(g_postconvert_res.value());

  KATANA_LOG_WARN("{}", g_preconvert->ReportDiff(g_postconvert.get()));

  KATANA_LOG_ASSERT(g_preconvert->Equals(g_postconvert.get()));
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  TestLoadGraphWithoutExternalTypes();

  return 0;
}

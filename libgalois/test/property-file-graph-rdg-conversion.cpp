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

std::string
StoreGraph(katana::PropertyGraph* g){
  auto uri_res = katana::Uri::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  std::string tmp_rdg_dir(uri_res.value().path());  // path() because local
  std::string command_line;

  // Store graph. If there is a new storage format then storing it is enough to bump the version up.
  KATANA_LOG_WARN("writing graph at temp file {}", tmp_rdg_dir);
  auto write_result = g->Write(tmp_rdg_dir, command_line);
  if (!write_result) {
    KATANA_LOG_FATAL("writing result failed: {}", write_result.error());
  }
  return tmp_rdg_dir;
}

void
TestConvertGraphStorageFormat() {
  // Load existing "old" graph, which converts all uint8/bool properties into types
  // store it as a new file
  // load the new file
  // ensure the converted old graph, and the loaded new graph match
  katana::PropertyGraph g = LoadGraph(ldbc_003InputFile);
  std::string g2_rdg_file = StoreGraph(&g);
  katana::PropertyGraph g2 = LoadGraph(g2_rdg_file);

  KATANA_LOG_WARN("{}", g.ReportDiff(&g2));
  KATANA_LOG_ASSERT(g.Equals(&g2));
}

void
TestRoundTripNewStorageFormat() {
  // Test store/load cycle of a graph with the new storage format
  // To do this, we first must first convert an old graph.
  // Steps:
  // Load existing "old" graph, which converts all uint8/bool properties into types
  // Store it as a new file
  // Load the new file
  // Ensure the converted old graph, and the loaded new graph match.
  // this should be trivially true if TestLoadGraphWithoutExternalTypes() passed
  // Now store the new graph
  // Noad the new graph

  // first cycle converts old->new
  katana::PropertyGraph g = LoadGraph(ldbc_003InputFile);
  std::string g2_rdg_file = StoreGraph(&g);
  katana::PropertyGraph g2 = LoadGraph(g2_rdg_file);

  KATANA_LOG_WARN("{}", g.ReportDiff(&g2));
  KATANA_LOG_ASSERT(g.Equals(&g2));

  // second cycle doesn't do any conversion, but tests storing/loading a "new format" graph
  std::string g3_rdg_file = StoreGraph(&g2);
  katana::PropertyGraph g3 = LoadGraph(g3_rdg_file);

  KATANA_LOG_WARN("{}", g2.ReportDiff(&g3));
  KATANA_LOG_ASSERT(g2.Equals(&g3));
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  TestConvertGraphStorageFormat();
  TestRoundTripNewStorageFormat();

  return 0;
}

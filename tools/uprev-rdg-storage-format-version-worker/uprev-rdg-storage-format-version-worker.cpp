#include <string>

#include <boost/filesystem.hpp>

#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "tsuba/RDG.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

/* usage: ./rdg-uprev-storage-format-version.cpp <input-rdg> <output-path>
 *
 * loads the given input rdg and stores it in the output-path
 * validates that the input rdg and the output rdg match
 * used to uprev the testing inputs
 */

static cll::opt<std::string> InputFile(
    cll::Positional, cll::desc("<input rdg file>"), cll::Required);

static cll::opt<std::string> OutputFile(
    cll::Positional, cll::desc("<output rdg file>"), cll::Required);

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
StoreGraph(katana::PropertyGraph* g, std::string& output_path) {
  std::string command_line;
  // Store graph. If there is a new storage format then storing it is enough to bump the version up.
  KATANA_LOG_WARN("writing graph at file {}", output_path);
  auto write_result = g->Write(output_path, command_line);
  if (!write_result) {
    KATANA_LOG_FATAL("writing result failed: {}", write_result.error());
  }
  return output_path;
}

void
UprevGraph(std::string& input_rdg, std::string& output_path) {
  katana::PropertyGraph g = LoadGraph(input_rdg);
  std::string g2_rdg_file = StoreGraph(&g, output_path);
  katana::PropertyGraph g2 = LoadGraph(g2_rdg_file);

  if (!g.Equals(&g2)) {
    KATANA_LOG_WARN("{}", g.ReportDiff(&g2));
    KATANA_LOG_VASSERT(
        false,
        "in memory graph from load previous storage_format_version does not "
        "match in memory graph loaded from new storage_format_version");
  }

  KATANA_LOG_WARN("uprev of {} stored at {}", input_rdg, g2_rdg_file);
}

int
main(int argc, char** argv) {
  //TODO(emcginnis): can we run this utility in distributed mode? If so, this should be distributed memory.
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  KATANA_LOG_ASSERT(!InputFile.empty());
  UprevGraph(InputFile, OutputFile);

  return 0;
}

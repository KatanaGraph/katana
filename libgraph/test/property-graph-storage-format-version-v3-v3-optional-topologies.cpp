#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/GraphTopology.h"
#include "katana/Iterators.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/RDG.h"
#include "katana/Result.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "storage-format-version-optional-topologies.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

/*
 * Tests to validate optional topology storage added in storage_format_version=3
 * Ensure we can add & store optional topologies to a graph that is already storage_format_version=3
 * Input can be any rdg with storage_format_version == 3
 */

static cll::opt<std::string> ldbc_003InputFile(
    cll::Positional, cll::desc("<ldbc_003 input file>"), cll::Required);

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  auto res = katana::URI::Make(ldbc_003InputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", ldbc_003InputFile, res.error());
  }
  auto uri = res.value();

  TestOptionalTopologyStorageEdgeShuffleTopology(uri);
  TestOptionalTopologyStorageShuffleTopology(uri);
  TestOptionalTopologyStorageEdgeTypeAwareTopology(uri);
  return 0;
}

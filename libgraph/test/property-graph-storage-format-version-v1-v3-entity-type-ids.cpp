#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/filesystem.hpp>

#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/RDG.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "storage-format-version-entity-type-ids.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

/*
 * Tests to validate EntityTypeID storage added in storage_format_version = 2
 * Input can be any rdg with storage_format_version < 2
 */

static cll::opt<std::string> ldbc_003InputFile(
    cll::Positional, cll::desc("<ldbc_003 input file>"), cll::Required);

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  TestConvertGraphStorageFormat(ldbc_003InputFile);
  TestRoundTripNewStorageFormat(ldbc_003InputFile);

  return 0;
}

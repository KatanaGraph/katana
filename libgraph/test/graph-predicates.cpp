#include <llvm/Support/CommandLine.h>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/RDG.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"

namespace cll = llvm::cl;

static cll::opt<std::string> rmat10InputFile(
    cll::Positional, cll::desc("<rmat10 input file>"), cll::Required);

void
TestIsApproximateDegreeDistributionPowerLaw() {
  katana::TxnContext txn_ctx;
  {
    LinePolicy policy{11};
    auto g = MakeFileGraph<uint32_t>(100, 1, &policy, &txn_ctx);

    KATANA_LOG_ASSERT(g->size() == 100);
    KATANA_LOG_ASSERT(g->NumEdges() == 11 * 100);

    KATANA_LOG_ASSERT(
        !katana::analytics::IsApproximateDegreeDistributionPowerLaw(*g.get()));
  }
  {
    auto g = katana::PropertyGraph::Make(
        rmat10InputFile, &txn_ctx, katana::RDGLoadOptions());

    KATANA_LOG_ASSERT(
        katana::analytics::IsApproximateDegreeDistributionPowerLaw(
            *g.assume_value().get()));
  }
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  TestIsApproximateDegreeDistributionPowerLaw();

  return 0;
}

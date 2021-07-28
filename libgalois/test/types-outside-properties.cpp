#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "tsuba/RDG.h"

namespace cll = llvm::cl;

static cll::opt<std::string> ldbc_003InputFile(
    cll::Positional, cll::desc("<ldbc_003 input file>"), cll::Required);

void
TestLoadGraphWithoutExternalTypes() {
  // Load existing "old" graph.
  // auto g =
  //     s katana::PropertyGraph::Make(ldbc_003InputFile, tsuba::RDGLoadOptions());

  // std::string converted_graph = "/tmp/converted_ldbc_003";
  // std::string command_line;

  // // TODO: (emcginnis) do some validation?
  // // Store new converted graph
  // g.WriteGraph(converted_graph, command_line)

  //     g = katana::PropertyGraph::Make(converted_graph, tsuba::RDGLoadOptions());
  // TODO: (emcginnis) do some validation?
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  TestLoadGraphWithoutExternalTypes();

  return 0;
}

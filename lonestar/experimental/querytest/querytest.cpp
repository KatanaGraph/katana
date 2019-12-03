#include "galois/Galois.h"
#include "galois/gstl.h"
#include "galois/Reduction.h"
#include "galois/Timer.h"
#include "galois/Timer.h"
#include "galois/graphs/LCGraph.h"
#include "galois/graphs/TypeTraits.h"
#include "llvm/Support/CommandLine.h"

#include "Lonestar/BoilerPlate.h"

// Querying all included by this
#include "DBGraph.h"

////////////////////////////////////////////////////////////////////////////////
// Benchmark metadata
////////////////////////////////////////////////////////////////////////////////

static const char* name = "DBGraph Testing";
static const char* desc = "Testing DBGraph";
static const char* url = "";

////////////////////////////////////////////////////////////////////////////////
// Command line args
////////////////////////////////////////////////////////////////////////////////

namespace cll = llvm::cl;

static cll::opt<std::string>
    filename(cll::Positional, cll::desc("<input graph>"), cll::Required);

////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
  galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url);

  // test reading
  galois::graphs::DBGraph testGraph;
  testGraph.readGr(filename);

  return 0;
}

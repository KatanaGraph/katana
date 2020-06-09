#include "galois/Galois.h"
#include "galois/gstl.h"
#include "Lonestar/BoilerPlate.h"

// Querying includes
#include "querying/DBGraph.h"
#include "querying/LDBCReader.h"

////////////////////////////////////////////////////////////////////////////////
// Benchmark metadata
////////////////////////////////////////////////////////////////////////////////

static const char* name = "LDBC Loader";
static const char* desc = "Loads LDBC data into memory and saves it to disk";
static const char* url  = "";

////////////////////////////////////////////////////////////////////////////////
// Command line args
////////////////////////////////////////////////////////////////////////////////

namespace cll = llvm::cl;

static cll::opt<std::string> ldbcDir(cll::Positional,
                                     cll::desc("LDBC root directory location"),
                                     cll::Required);

static cll::opt<uint32_t> numNodes("numNodes",
                                   cll::desc("number of nodes in dataset"),
                                   cll::Required);

static cll::opt<uint64_t> numEdges("numEdges",
                                   cll::desc("number of edges in dataset"),
                                   cll::Required);

////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
  galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url, ldbcDir.c_str());

  LDBCReader reader(ldbcDir, numNodes, numEdges);
  reader.staticParsing();

  return 0;
}

#include "galois/Galois.h"
#include "galois/gstl.h"
#include "galois/Reduction.h"
#include "galois/Timer.h"
#include "galois/Timer.h"
#include "galois/graphs/LCGraph.h"
#include "galois/graphs/TypeTraits.h"

#include "Lonestar/BoilerPlate.h"

// Querying all included by this single header
#include "querying/DBGraph.h"

////////////////////////////////////////////////////////////////////////////////
// Benchmark metadata
////////////////////////////////////////////////////////////////////////////////

static const char* name = "DBGraph Testing";
static const char* desc = "Testing DBGraph";
static const char* url  = "";

////////////////////////////////////////////////////////////////////////////////
// Command line args
////////////////////////////////////////////////////////////////////////////////

namespace cll = llvm::cl;

static cll::opt<std::string>
    filename(cll::Positional, cll::desc("<input graph>"), cll::Required);

static cll::opt<std::string> query("query", cll::desc("Cypher query"),
                                   cll::init(""));

// TODO get this to work; requires library end to accept argument to where for
// output
// static cll::opt<std::string>
//    edgefile(cll::Positional, cll::desc("Cypher query"), cll::Required);
//

static cll::opt<bool> isAttributedGraph(
    "isAttributedGraph",
    cll::desc(
        "Specifies that the passed in file is an attributed graph on disk "
        "(default false)"),
    cll::init(false));

static cll::opt<std::string>
    queryFile("queryFile",
              cll::desc("File containing Cypher query to run"
                        "; takes precedence over query string"),
              cll::init(""));

static cll::opt<bool>
    skipGraphSimulation("skipGraphSimulation",
                        cll::desc("Do not use graph simulation "
                                  "(default false)"),
                        cll::init(false));

static cll::opt<uint32_t> numPages("numPages",
                                   cll::desc("Number of pages to pre-alloc "
                                             "(default 2500)"),
                                   cll::init(2500));

////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
  galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url, &filename);

  galois::graphs::DBGraph testGraph;
  // graph is automatically made symmetric and treats every directed edge
  // as an undirected edge (i.e. edges will be doubled)
  // Also removes self loops
  if (!isAttributedGraph) {
    testGraph.constructDataGraph(filename);
  } else {
    testGraph.loadSerializedAttributedGraph(filename);
  }

  galois::preAlloc(numPages);
  galois::reportPageAlloc("MeminfoPre");

  // current assumptions of the graph
  // 3 node labels: n1, n2, n3
  // 3 edge labels: e1, e2, e3
  // timestamps on edges are increasing order
  if (queryFile != "") {
    // read file into a std::string
    // https://stackoverflow.com/questions/2602013/read-whole-ascii-file-into-c-stdstring
    // TODO check if opened successfully (good C practice to do so)
    std::ifstream queryStream(queryFile);
    std::stringstream querySS;
    // putting into string stream lets you pull a string out of it
    querySS << queryStream.rdbuf();

    galois::gInfo(
        "Num matched subgraphs ",
        testGraph.runCypherQuery(querySS.str(), !skipGraphSimulation));
  } else if (query != "") {
    galois::gInfo("Num matched subgraphs ",
                  testGraph.runCypherQuery(query, !skipGraphSimulation));
  } else {
    galois::gInfo("No query specified");
  }

  galois::reportPageAlloc("MeminfoPost");

  return 0;
}

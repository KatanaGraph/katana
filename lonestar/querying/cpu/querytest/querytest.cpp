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

static const char* name = "Query Test";
static const char* desc = "Run queries on a given property graph";
static const char* url  = "";

////////////////////////////////////////////////////////////////////////////////
// Command line args
////////////////////////////////////////////////////////////////////////////////

namespace cll = llvm::cl;

static cll::opt<std::string>
    filename(cll::Positional, cll::desc("<input graph>"), cll::Required);

static cll::opt<std::string> query("query", cll::desc("Cypher query"),
                                   cll::init(""));

static cll::opt<bool> isAttributedGraph(
    "isAttributedGraph",
    cll::desc(
        "Specifies that the passed in file is an attributed graph on disk "
        "(default false)"),
    cll::init(false));

static cll::opt<std::string> listOfQueries(
    "listOfQueries",
    cll::desc("File containing a list of files with Cypher queries to "
              "run takes highest precedence of all input methods"),
    cll::init(""));

static cll::opt<std::string>
    queryFile("queryFile",
              cll::desc("File containing Cypher query to run"
                        "; takes precedence over query string"),
              cll::init(""));

static cll::opt<bool>
    output("output",
           cll::desc("If true, write query count to file; only works if using "
                     "listOfQueries argument (default false)"),
           cll::init(false));

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

//! given file with query, run query and return number of matches
size_t processQueryFile(galois::graphs::DBGraph& testGraph,
                        std::string fileToProcess,
                        std::string queryName = "Query") {
  galois::gInfo("Reading query file ", fileToProcess);
  // read file into a std::string
  // https://stackoverflow.com/questions/2602013/read-whole-ascii-file-into-c-stdstring
  std::ifstream queryStream(fileToProcess);
  if (!queryStream.is_open()) {
    GALOIS_DIE("failed to open query file ", fileToProcess);
  }
  std::stringstream querySS;
  // putting into string stream lets you pull a string out of it
  querySS << queryStream.rdbuf();

  galois::StatTimer timer((queryName + "_Timer").c_str());
  timer.start();
  size_t numMatch =
      testGraph.runCypherQuery(querySS.str(), !skipGraphSimulation);
  timer.stop();

  galois::gInfo("Num matched subgraphs ", numMatch);
  return numMatch;
}

int main(int argc, char** argv) {
  galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url, &filename);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();


  galois::graphs::DBGraph testGraph;
  if (!isAttributedGraph) {
    // current assumptions of the graph if not using an attributed graph
    // 3 node labels: n1, n2, n3
    // 3 edge labels: e1, e2, e3
    // timestamps on edges are increasing order
    // graph is automatically made symmetric and treats every directed edge
    // as an undirected edge (i.e. edges will be doubled)
    // Also removes self loops
    testGraph.constructDataGraph(filename);
  } else {
    // uses an attributed graph saved on disk via the interface present in
    // attributed graph
    testGraph.loadSerializedAttributedGraph(filename);
  }

  galois::preAlloc(numPages);
  galois::reportPageAlloc("MeminfoPre");

  if (listOfQueries != "") {
    galois::gInfo("Reading list of query files ", listOfQueries);
    std::ifstream queryFiles(listOfQueries);
    if (!queryFiles.is_open()) {
      GALOIS_DIE("failed to open query list file ", listOfQueries);
    }

    std::ofstream outputFile("queries.count");

    std::string curQueryFile;
    while (std::getline(queryFiles, curQueryFile)) {
      // TODO drop file extension as well
      std::string queryName =
          curQueryFile.substr(curQueryFile.find_last_of("/\\") + 1);
      size_t matched = processQueryFile(testGraph, curQueryFile, queryName);

      // save query name and count for correctness checking
      if (output) {
        outputFile << queryName << " " << matched << "\n";
      }
    }
  } else if (queryFile != "") {
    processQueryFile(testGraph, queryFile);
  } else if (query != "") {
    galois::gInfo("Num matched subgraphs ",
                  testGraph.runCypherQuery(query, !skipGraphSimulation));
  } else {
    galois::gInfo("No query specified");
  }

  galois::reportPageAlloc("MeminfoPost");

  // TODO make this work regardless of how you pass in your queries
  if (listOfQueries != "" && output) {
    galois::gInfo("Query counts saved to queries.count");
  }

  totalTime.stop();
  return 0;
}

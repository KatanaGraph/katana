#include "galois/Galois.h"
#include "galois/gstl.h"
#include "galois/Timer.h"
#include "Lonestar/BoilerPlate.h"

// Querying things all included by this single header (mainly access to the
// attributed graph is via this header
#include "querying/KFHandler.h"

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
    filename(cll::Positional,
             cll::desc("<Katana Form metafile or Boost-serialized graph>"),
             cll::Required);

static cll::opt<std::string> query("query", cll::desc("Cypher query"),
                                   cll::init(""));

static cll::opt<bool> isBoostSerialized(
    "boostSerialized",
    cll::desc(
        "Specifies that the passed in-file is a boost serialized graph on disk "
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

static cll::opt<std::string> outputLocation(
    "outputLocation",
    cll::desc("Location (directory) to write output if output is true"),
    cll::init("./"));

static cll::opt<uint32_t> numPages("numPages",
                                   cll::desc("Number of pages to pre-alloc "
                                             "(default 2500)"),
                                   cll::init(2500));

////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////

//! given file with query, run query and return number of matches
size_t processQueryFile(galois::graphs::AttributedGraph& att_graph,
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
  // size_t numMatch = testGraph.runCypherQuery(querySS.str());
  size_t numMatch = att_graph.matchCypherQuery(querySS.str().c_str());
  timer.stop();

  galois::gInfo("Num matched subgraphs ", numMatch);
  return numMatch;
}

////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
  galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url, &filename);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  galois::gInfo("Constructing graph");

  // load graph
  galois::StatTimer graphConstructTime("GraphConstructTime");
  graphConstructTime.start();
  galois::graphs::AttributedGraph att_graph;
  if (!isBoostSerialized) {
    querying::loadKatanaFormToCypherGraph(att_graph, filename);
  } else {
    // boost serialized graph
    att_graph.loadGraph(filename.c_str());
  }
  graphConstructTime.stop();
  att_graph.reportGraphStats();

  galois::preAlloc(numPages);
  galois::reportPageAlloc("MeminfoPre");

  galois::gInfo("Running query/queries");
  galois::StatTimer mainTimer("Timer_0");
  mainTimer.start();

  // do querying proper; determine how the query was passed into the program
  if (listOfQueries != "") {
    galois::gInfo("Reading list of query files ", listOfQueries);
    std::ifstream queryFiles(listOfQueries);
    if (!queryFiles.is_open()) {
      GALOIS_DIE("failed to open query list file ", listOfQueries);
    }

    std::ofstream outputFile(outputLocation + "/queries.count");
    std::string curQueryFile;

    while (std::getline(queryFiles, curQueryFile)) {
      // TODO drop file extension as well
      std::string queryName =
          curQueryFile.substr(curQueryFile.find_last_of("/\\") + 1);
      size_t matched = processQueryFile(att_graph, curQueryFile, queryName);

      // save query name and count for correctness checking
      if (output) {
        outputFile << queryName << " " << matched << "\n";
      }
    }
  } else if (queryFile != "") {
    processQueryFile(att_graph, queryFile);
  } else if (query != "") {
    galois::StatTimer timer("Query_Timer");
    timer.start();
    galois::gInfo("Num matched subgraphs ",
                  att_graph.matchCypherQuery(query.c_str()));
    timer.stop();
  } else {
    galois::gWarn("No query specified");
  }
  mainTimer.stop();

  galois::reportPageAlloc("MeminfoPost");

  galois::gInfo("Querying complete");

  // TODO make this work regardless of how you pass in your queries
  if (listOfQueries != "" && output) {
    galois::gInfo("Query counts saved to ", outputLocation, "/queries.count");
  }

  totalTime.stop();

  return 0;
}

#include "graph-properties-convert.h"
#include "galois/Galois.h"
#include "galois/Timer.h"
#include "galois/ErrorCode.h"
#include "galois/Logging.h"

#include <llvm/Support/CommandLine.h>

namespace cll = llvm::cl;

static cll::opt<std::string> inputFilename(cll::Positional,
                                           cll::desc("<input file/directory>"),
                                           cll::Required);
static cll::opt<std::string>
    outputDirectory(cll::Positional,
                    cll::desc("<local ouput directory/s3 directory>"),
                    cll::Required);
static cll::opt<galois::SourceType>
    type(cll::desc("Input file type:"),
         cll::values(clEnumValN(galois::SourceType::GRAPHML, "graphml",
                                "source file is of type GraphML"),
                     clEnumValN(galois::SourceType::JSON, "json",
                                "source file is of type JSON"),
                     clEnumValN(galois::SourceType::CSV, "csv",
                                "source file is of type CSV")),
         cll::Required);
static cll::opt<galois::SourceDatabase>
    database(cll::desc("Database the data was exported from:"),
             cll::values(clEnumValN(galois::SourceDatabase::NEO4J, "neo4j",
                                    "source data came from Neo4j"),
                         clEnumValN(galois::SourceDatabase::MONGODB, "mongodb",
                                    "source data came from mongodb")),
             cll::init(galois::SourceDatabase::NONE));
static cll::opt<size_t>
    chunkSize("chunkSize",
              cll::desc("Chunk size for in memory arrow representation"),
              cll::init(25000));

void parseWild() {
  switch (type) {
  case galois::SourceType::GRAPHML: {
    auto graph = galois::convertGraphML(inputFilename, chunkSize);
    galois::convertToPropertyGraphAndWrite(graph, outputDirectory);
    break;
  }
  default: {
    GALOIS_LOG_ERROR("Only graphml files are supported for wild datasets");
  }
  }
}

void parseNeo4j() {
  galois::GraphComponents graph{nullptr, nullptr, nullptr, nullptr, nullptr};
  switch (type) {
  case galois::SourceType::GRAPHML:
    graph = galois::convertGraphML(inputFilename, chunkSize);
    break;
  case galois::SourceType::JSON:
    graph = galois::convertNeo4jJSON(inputFilename);
    break;
  case galois::SourceType::CSV:
    graph = galois::convertNeo4jCSV(inputFilename);
    break;
  }
  galois::convertToPropertyGraphAndWrite(graph, outputDirectory);
}

void parseMongodb() {
  switch (type) {
  case galois::SourceType::JSON:
    GALOIS_LOG_WARN("MongoDB importing is under development");
    break;
  default:
    GALOIS_LOG_ERROR("Only json files are supported for MongoDB exports");
  }
}

int main(int argc, char** argv) {
  galois::SharedMemSys sys;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  galois::StatTimer totalTimer("TimerTotal");
  totalTimer.start();

  switch (database) {
  case galois::SourceDatabase::NONE:
    parseWild();
    break;
  case galois::SourceDatabase::NEO4J:
    parseNeo4j();
    break;
  case galois::SourceDatabase::MONGODB:
    parseMongodb();
    break;
  }

  totalTimer.stop();
}

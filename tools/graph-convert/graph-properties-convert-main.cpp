#include "graph-properties-convert.h"

#include <llvm/Support/CommandLine.h>
#include "galois/ErrorCode.h"
#include "galois/Logging.h"

namespace cll = llvm::cl;

static cll::opt<std::string> inputFilename(cll::Positional,
                                           cll::desc("<input file/directory>"),
                                           cll::Required);
static cll::opt<std::string>
    outputDirectory(cll::Positional,
                    cll::desc("<local ouput directory/s3 directory>"),
                    cll::Required);
static cll::opt<convert::SourceType>
    type(cll::desc("Input file type:"),
         cll::values(clEnumValN(convert::SourceType::GRAPHML, "graphml",
                                "source file is of type GraphML"),
                     clEnumValN(convert::SourceType::JSON, "json",
                                "source file is of type JSON"),
                     clEnumValN(convert::SourceType::CSV, "csv",
                                "source file is of type CSV")),
         cll::Required);
static cll::opt<convert::SourceDatabase>
    database(cll::desc("Database the data was exported from:"),
             cll::values(clEnumValN(convert::SourceDatabase::NEO4J, "neo4j",
                                    "source data came from Neo4j"),
                         clEnumValN(convert::SourceDatabase::MONGODB, "mongodb",
                                    "source data came from mongodb")),
             cll::init(convert::SourceDatabase::NONE));

namespace convert {
namespace main {

void parseWild() {
  switch (type) {
  case convert::SourceType::GRAPHML: {
    auto graph = convert::convertGraphML(inputFilename);
    convert::convertToPropertyGraphAndWrite(graph, outputDirectory);
    break;
  }
  default: {
    GALOIS_LOG_ERROR("Only graphml files are supported for wild datasets");
  }
  }
}

void parseNeo4j() {
  convert::GraphComponents graph{nullptr, nullptr, nullptr, nullptr, nullptr};
  switch (type) {
  case convert::SourceType::GRAPHML:
    graph = convert::convertGraphML(inputFilename);
    break;
  case convert::SourceType::JSON:
    graph = convert::convertNeo4jJSON(inputFilename);
    break;
  case convert::SourceType::CSV:
    graph = convert::convertNeo4jCSV(inputFilename);
    break;
  }
  convert::convertToPropertyGraphAndWrite(graph, outputDirectory);
}

void parseMongodb() {
  switch (type) {
  case convert::SourceType::JSON: // convertMongoDB(inputFilename,
                                  // outputDirectory);
    GALOIS_LOG_WARN("MongoDB importing is under development");
    break;
  default:
    GALOIS_LOG_ERROR("Only json files are supported for MongoDB exports");
  }
}

} // end of namespace main
} // end of namespace convert

int main(int argc, char** argv) {
  llvm::cl::ParseCommandLineOptions(argc, argv);

  switch (database) {
  case convert::SourceDatabase::NONE:
    convert::main::parseWild();
    break;
  case convert::SourceDatabase::NEO4J:
    convert::main::parseNeo4j();
    break;
  case convert::SourceDatabase::MONGODB:
    convert::main::parseMongodb();
    break;
  }
}
#include <iostream>

#include <llvm/Support/CommandLine.h>

#include "Transforms.h"
#include "galois/ErrorCode.h"
#include "galois/Galois.h"
#include "galois/Logging.h"
#include "galois/Timer.h"
#include "galois/config.h"
#include "graph-properties-convert-graphml.h"
#include "graph-properties-convert-schema.h"

#if defined(GALOIS_MONGOC_FOUND)
#include "graph-properties-convert-mongodb.h"
#endif
#if defined(GALOIS_MYSQL_FOUND)
#include "graph-properties-convert-mysql.h"
#endif

namespace cll = llvm::cl;

namespace {

cll::opt<std::string> input_filename(
    cll::Positional, cll::desc("<input file/directory>"), cll::Required);
cll::opt<std::string> output_directory(
    cll::Positional, cll::desc("<local output directory/s3 directory>"),
    cll::Required);
cll::opt<galois::SourceType> type(
    cll::desc("Input file type:"),
    cll::values(
        clEnumValN(
            galois::SourceType::kGraphml, "graphml",
            "source file is of type GraphML"),
        clEnumValN(
            galois::SourceType::kKatana, "katana",
            "source file is of type Katana")),
    cll::init(galois::SourceType::kGraphml));
cll::opt<galois::SourceDatabase> database(
    cll::desc("Database the data is from:"),
    cll::values(
        clEnumValN(
            galois::SourceDatabase::kNeo4j, "neo4j",
            "source data came from Neo4j"),
        clEnumValN(
            galois::SourceDatabase::kMongodb, "mongodb", "source is mongodb"),
        clEnumValN(galois::SourceDatabase::kMysql, "mysql", "source is mysql")),
    cll::init(galois::SourceDatabase::kNone));
cll::opt<int> chunk_size(
    "chunk-size",
    cll::desc("Chunk size for in memory arrow representation during "
              "conversions\n"
              "Generally this term can be ignored, but "
              "it can be decreased to improve memory usage when "
              "converting large inputs"),
    cll::init(25000));
cll::opt<std::string> mapping(
    "mapping",
    cll::desc("File in graphml format with a schema mapping for the database"),
    cll::init(""));
cll::opt<bool> generate_mapping(
    "generate-mapping",
    cll::desc("Generate a file in graphml format with a schema mapping for the "
              "database\n"
              "The file is created at the output destination specified"),
    cll::init(false));

cll::list<std::string> timestamp_properties(
    "timestamp", cll::desc("Timestamp properties"));
cll::list<std::string> date32_properties(
    "date32", cll::desc("Date32 properties"));
cll::list<std::string> date64_properties(
    "date64", cll::desc("Date64 properties"));

cll::opt<std::string> host(
    "host",
    cll::desc("URL/IP/localhost for the target database if needed, "
              "default is 127.0.0.1"),
    cll::init("127.0.0.1"));
cll::opt<std::string> user(
    "user",
    cll::desc("Username for the target database if needed, default is root"),
    cll::init("root"));

cll::opt<bool> export_graphml(
    "export",
    cll::desc("Exports a Katana graph to graphml format\n"
              "The file is created at the output destination specified\n"),
    cll::init(false));

galois::graphs::PropertyFileGraph
ConvertKatana(const std::string& rdg_file) {
  auto result = galois::graphs::PropertyFileGraph::Make(rdg_file);
  if (!result) {
    GALOIS_LOG_FATAL("failed to load {}: {}", rdg_file, result.error());
  }

  std::unique_ptr<galois::graphs::PropertyFileGraph> graph =
      std::move(result.value());

  std::vector<std::unique_ptr<galois::ColumnTransformer>> transformers;

  transformers.emplace_back(std::make_unique<galois::SparsifyBooleans>());

  if (!timestamp_properties.empty()) {
    std::vector<std::string> t_fields(
        timestamp_properties.begin(), timestamp_properties.end());
    // Technically, a Unix timestamp is not in UTC because it does not account
    // for leap seconds since the beginning of the epoch. Parquet and arrow use
    // Unix timestamps throughout so they also avoid accounting for this
    // distinction.
    // TODO(danielmawhirter) leap seconds
    transformers.emplace_back(std::make_unique<galois::ConvertDateTime>(
        arrow::timestamp(arrow::TimeUnit::NANO, "UTC"), t_fields));
  }

  if (!date32_properties.empty()) {
    std::vector<std::string> t_fields(
        date32_properties.begin(), date32_properties.end());
    transformers.emplace_back(
        std::make_unique<galois::ConvertDateTime>(arrow::date32(), t_fields));
  }

  if (!date64_properties.empty()) {
    std::vector<std::string> t_fields(
        date64_properties.begin(), date64_properties.end());
    transformers.emplace_back(
        std::make_unique<galois::ConvertDateTime>(arrow::date64(), t_fields));
  }

  ApplyTransforms(graph.get(), transformers);

  return galois::graphs::PropertyFileGraph(std::move(*graph));
}

void
ParseWild() {
  switch (type) {
  case galois::SourceType::kGraphml:
    return galois::WritePropertyGraph(
        galois::ConvertGraphML(input_filename, chunk_size), output_directory);
  case galois::SourceType::kKatana:
    return galois::WritePropertyGraph(
        ConvertKatana(input_filename), output_directory);
  default:
    GALOIS_LOG_ERROR("Unsupported input type {}", type);
  }
}

void
ParseNeo4j() {
  switch (type) {
  case galois::SourceType::kGraphml:
    return galois::WritePropertyGraph(
        galois::ConvertGraphML(input_filename, chunk_size), output_directory);
  default:
    GALOIS_LOG_ERROR("Unsupported input type {}", type);
  }
}

void
ParseMongoDB() {
#if defined(GALOIS_MONGOC_FOUND)
  if (generate_mapping) {
    galois::GenerateMappingMongoDB(input_filename, output_directory);
  } else {
    galois::WritePropertyGraph(
        galois::ConvertMongoDB(input_filename, mapping, chunk_size),
        output_directory);
  }
#else
  GALOIS_LOG_FATAL("Dependencies not present for MongoDB");
#endif
}

void
ParseMysql() {
#if defined(GALOIS_MYSQL_FOUND)
  if (generate_mapping) {
    galois::GenerateMappingMysql(input_filename, output_directory, host, user);
  } else {
    galois::WritePropertyGraph(
        galois::ConvertMysql(input_filename, mapping, chunk_size, host, user),
        output_directory);
  }
#else
  GALOIS_LOG_FATAL("Dependencies not present for MySQL");
#endif
}

}  // namespace

int
main(int argc, char** argv) {
  galois::SharedMemSys sys;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  galois::StatTimer total_timer("TimerTotal");
  total_timer.start();
  if (chunk_size <= 0) {
    chunk_size = 25000;
  }

  if (export_graphml) {
    galois::ExportGraph(output_directory, input_filename);
  } else {
    switch (database) {
    case galois::SourceDatabase::kNone:
      ParseWild();
      break;
    case galois::SourceDatabase::kNeo4j:
      ParseNeo4j();
      break;
    case galois::SourceDatabase::kMongodb:
      ParseMongoDB();
      break;
    case galois::SourceDatabase::kMysql:
      ParseMysql();
      break;
    }
  }

  total_timer.stop();
}

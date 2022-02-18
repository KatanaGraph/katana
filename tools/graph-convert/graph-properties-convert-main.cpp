#include <iostream>

#include <llvm/Support/CommandLine.h>

#include "Transforms.h"
#include "katana/ErrorCode.h"
#include "katana/Galois.h"
#include "katana/GraphML.h"
#include "katana/GraphMLSchema.h"
#include "katana/Logging.h"
#include "katana/RDG.h"
#include "katana/SharedMemSys.h"
#include "katana/Timer.h"
#include "katana/config.h"

#if defined(KATANA_MONGOC_FOUND)
#include "graph-properties-convert-mongodb.h"
#endif
#if defined(KATANA_MYSQL_FOUND)
#include "graph-properties-convert-mysql.h"
#endif

namespace cll = llvm::cl;

namespace {

cll::opt<std::string> input_filename(
    cll::Positional, cll::desc("<input file/directory>"), cll::Required);
cll::opt<std::string> output_directory(
    cll::Positional, cll::desc("<local output directory/s3 directory>"),
    cll::Required);
cll::opt<katana::SourceType> type(
    cll::desc("Input file type:"),
    cll::values(
        clEnumValN(
            katana::SourceType::kGraphml, "graphml",
            "source file is of type GraphML"),
        clEnumValN(
            katana::SourceType::kKatana, "katana",
            "source file is of type Katana")),
    cll::init(katana::SourceType::kGraphml));
cll::opt<katana::SourceDatabase> database(
    cll::desc("Database the data is from:"),
    cll::values(
        clEnumValN(
            katana::SourceDatabase::kNeo4j, "neo4j",
            "source data came from Neo4j"),
        clEnumValN(
            katana::SourceDatabase::kMongodb, "mongodb", "source is mongodb"),
        clEnumValN(katana::SourceDatabase::kMysql, "mysql", "source is mysql")),
    cll::init(katana::SourceDatabase::kNone));
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

std::unique_ptr<katana::PropertyGraph>
ConvertKatana(const std::string& rdg_file, katana::TxnContext* txn_ctx) {
  auto result =
      katana::PropertyGraph::Make(rdg_file, txn_ctx, katana::RDGLoadOptions());
  if (!result) {
    KATANA_LOG_FATAL("failed to load {}: {}", rdg_file, result.error());
  }

  std::unique_ptr<katana::PropertyGraph> graph = std::move(result.value());

  std::vector<std::unique_ptr<katana::ColumnTransformer>> transformers;

  transformers.emplace_back(std::make_unique<katana::SparsifyBooleans>());

  if (!timestamp_properties.empty()) {
    std::vector<std::string> t_fields(
        timestamp_properties.begin(), timestamp_properties.end());
    // Technically, a Unix timestamp is not in UTC because it does not account
    // for leap seconds since the beginning of the epoch. Parquet and arrow use
    // Unix timestamps throughout so they also avoid accounting for this
    // distinction.
    // TODO(danielmawhirter) leap seconds
    transformers.emplace_back(std::make_unique<katana::ConvertDateTime>(
        arrow::timestamp(arrow::TimeUnit::NANO, "UTC"), t_fields));
  }

  if (!date32_properties.empty()) {
    std::vector<std::string> t_fields(
        date32_properties.begin(), date32_properties.end());
    transformers.emplace_back(
        std::make_unique<katana::ConvertDateTime>(arrow::date32(), t_fields));
  }

  if (!date64_properties.empty()) {
    std::vector<std::string> t_fields(
        date64_properties.begin(), date64_properties.end());
    transformers.emplace_back(
        std::make_unique<katana::ConvertDateTime>(arrow::date64(), t_fields));
  }

  ApplyTransforms(graph.get(), transformers, txn_ctx);

  return graph;
}

void
ParseWild(katana::TxnContext* txn_ctx) {
  switch (type) {
  case katana::SourceType::kGraphml: {
    auto components_result =
        katana::ConvertGraphML(input_filename, chunk_size, true);
    if (!components_result) {
      KATANA_LOG_FATAL("Error converting graph: {}", components_result.error());
    }
    if (auto r = katana::WritePropertyGraph(
            std::move(components_result.value()), output_directory, txn_ctx);
        !r) {
      KATANA_LOG_FATAL("Failed to convert property graph: {}", r.error());
    }
    return;
  }
  case katana::SourceType::kKatana:
    if (auto r = katana::WritePropertyGraph(
            *ConvertKatana(input_filename, txn_ctx), output_directory, txn_ctx);
        !r) {
      KATANA_LOG_FATAL("Failed to convert property graph: {}", r.error());
    }
    return;
  default:
    KATANA_LOG_ERROR("Unsupported input type {}", type);
  }
}

void
ParseNeo4j(katana::TxnContext* txn_ctx) {
  switch (type) {
  case katana::SourceType::kGraphml: {
    auto components_result =
        katana::ConvertGraphML(input_filename, chunk_size, true);
    if (!components_result) {
      KATANA_LOG_FATAL("Error converting graph: {}", components_result.error());
    }
    if (auto r = katana::WritePropertyGraph(
            std::move(components_result.value()), output_directory, txn_ctx);
        !r) {
      KATANA_LOG_FATAL("Failed to convert property graph: {}", r.error());
    }
    return;
  }
  default:
    KATANA_LOG_ERROR("Unsupported input type {}", type);
  }
}

void
ParseMongoDB([[maybe_unused]] katana::TxnContext* txn_ctx) {
#if defined(KATANA_MONGOC_FOUND)
  if (generate_mapping) {
    katana::GenerateMappingMongoDB(input_filename, output_directory);
  } else {
    if (auto r = katana::WritePropertyGraph(
            katana::ConvertMongoDB(input_filename, mapping, chunk_size),
            output_directory, txn_ctx);
        !r) {
      KATANA_LOG_FATAL("Failed to write property graph: {}", r.error());
    }
  }
#else
  KATANA_LOG_FATAL("Dependencies not present for MongoDB");
#endif
}

void
ParseMysql([[maybe_unused]] katana::TxnContext* txn_ctx) {
#if defined(KATANA_MYSQL_FOUND)
  if (generate_mapping) {
    katana::GenerateMappingMysql(input_filename, output_directory, host, user);
  } else {
    if (auto r = katana::WritePropertyGraph(
            katana::ConvertMysql(
                input_filename, mapping, chunk_size, host, user),
            output_directory, txn_ctx);
        !r) {
      KATANA_LOG_FATAL("Failed to write property graph: {}", r.error());
    }
  }
#else
  KATANA_LOG_FATAL("Dependencies not present for MySQL");
#endif
}

}  // namespace

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  katana::StatTimer total_timer("TimerTotal");
  total_timer.start();
  if (chunk_size <= 0) {
    chunk_size = 25000;
  }

  katana::TxnContext txn_ctx;
  if (export_graphml) {
    katana::graphml::ExportGraph(output_directory, input_filename, &txn_ctx);
  } else {
    switch (database) {
    case katana::SourceDatabase::kNone:
      ParseWild(&txn_ctx);
      break;
    case katana::SourceDatabase::kNeo4j:
      ParseNeo4j(&txn_ctx);
      break;
    case katana::SourceDatabase::kMongodb:
      ParseMongoDB(&txn_ctx);
      break;
    case katana::SourceDatabase::kMysql:
      ParseMysql(&txn_ctx);
      break;
    }
  }

  total_timer.stop();
}

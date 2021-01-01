#include <iostream>
#include <memory>

#include <llvm/Support/CommandLine.h>

#include "graph-properties-convert-graphml.h"
#include "katana/Galois.h"
#include "katana/Logging.h"
#include "katana/config.h"

#if defined(KATANA_MONGOC_FOUND)
#include "graph-properties-convert-mongodb.h"
#endif

namespace {
enum ConvertTest { kMovies, kTypes, kChunks, kMongodb };
}

namespace cll = llvm::cl;

static cll::opt<std::string> input_filename(
    cll::Positional, cll::desc("<input file/directory>"), cll::Required);
static cll::opt<katana::SourceDatabase> fileType(
    cll::desc("Input file type:"),
    cll::values(clEnumValN(
        katana::SourceDatabase::kNeo4j, "neo4j", "source file is from neo4j")),
    cll::values(clEnumValN(
        katana::SourceDatabase::kMongodb, "mongodb", "source is from MongoDB")),
    cll::Required);
static cll::opt<ConvertTest> test_type(
    cll::desc("Input file type:"),
    cll::values(
        clEnumValN(
            ConvertTest::kTypes, "types",
            "source file is a test for graphml type conversion"),
        clEnumValN(
            ConvertTest::kMovies, "movies",
            "source file is a test for generic conversion"),
        clEnumValN(ConvertTest::kChunks, "chunks", "this is a test for chunks"),
        clEnumValN(
            ConvertTest::kMongodb, "mongo", "this is a test for mongodb")),
    cll::Required);
static cll::opt<int> chunk_size(
    "chunkSize", cll::desc("Chunk size for in memory arrow representation"),
    cll::init(25000));

namespace {

template <typename T, typename U>
std::shared_ptr<T>
safe_cast(const std::shared_ptr<U>& r) noexcept {
  auto p = std::dynamic_pointer_cast<T>(r);
  KATANA_LOG_ASSERT(p);
  return p;
}

void
VerifyMovieSet(const katana::GraphComponents& graph) {
  KATANA_ASSERT(graph.nodes.properties->num_columns() == 5);
  KATANA_ASSERT(graph.nodes.labels->num_columns() == 4);
  KATANA_ASSERT(graph.edges.properties->num_columns() == 2);
  KATANA_ASSERT(graph.edges.labels->num_columns() == 4);

  KATANA_ASSERT(graph.nodes.properties->num_rows() == 9);
  KATANA_ASSERT(graph.nodes.labels->num_rows() == 9);
  KATANA_ASSERT(graph.edges.properties->num_rows() == 8);
  KATANA_ASSERT(graph.edges.labels->num_rows() == 8);

  KATANA_ASSERT(graph.topology->out_indices->length() == 9);
  KATANA_ASSERT(graph.topology->out_dests->length() == 8);

  // test node properties
  auto names = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("name")->chunk(0));
  std::string names_expected = std::string(
      "[\n\
  null,\n\
  \"Keanu Reeves\",\n\
  \"Carrie-Anne Moss\",\n\
  \"Laurence Fishburne\",\n\
  \"Hugo Weaving\",\n\
  \"Lilly Wachowski\",\n\
  \"Lana Wachowski\",\n\
  \"Joel Silver\",\n\
  null\n\
]");
  KATANA_ASSERT(names->ToString() == names_expected);

  auto taglines = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("tagline")->chunk(0));
  std::string taglines_expected = std::string(
      "[\n\
  \"Welcome to the Real World\",\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(taglines->ToString() == taglines_expected);

  auto titles = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("title")->chunk(0));
  std::string titles_expected = std::string(
      "[\n\
  \"The Matrix\",\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(titles->ToString() == titles_expected);

  auto released = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("released")->chunk(0));
  std::string released_expected = std::string(
      "[\n\
  \"1999\",\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(released->ToString() == released_expected);

  auto borns = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("born")->chunk(0));
  std::string borns_expected = std::string(
      "[\n\
  null,\n\
  \"1964\",\n\
  \"1967\",\n\
  \"1961\",\n\
  \"1960\",\n\
  \"1967\",\n\
  \"1965\",\n\
  \"1952\",\n\
  \"1963\"\n\
]");
  KATANA_ASSERT(borns->ToString() == borns_expected);

  // test node labels
  auto movies = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("Movie")->chunk(0));
  std::string movies_expected = std::string(
      "[\n\
  true,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false\n\
]");
  KATANA_ASSERT(movies->ToString() == movies_expected);

  auto persons = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("Person")->chunk(0));
  std::string persons_expected = std::string(
      "[\n\
  false,\n\
  true,\n\
  true,\n\
  true,\n\
  true,\n\
  true,\n\
  true,\n\
  true,\n\
  true\n\
]");
  KATANA_ASSERT(persons->ToString() == persons_expected);

  auto others = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("Other")->chunk(0));
  std::string others_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  KATANA_ASSERT(others->ToString() == others_expected);

  auto randoms = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("Random")->chunk(0));
  std::string randoms_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  KATANA_ASSERT(randoms->ToString() == randoms_expected);

  // test edge properties
  auto roles = safe_cast<arrow::StringArray>(
      graph.edges.properties->GetColumnByName("roles")->chunk(0));
  std::string roles_expected = std::string(
      "[\n\
  \"Neo\",\n\
  \"Trinity\",\n\
  \"Morpheus\",\n\
  null,\n\
  \"Agent Smith\",\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(roles->ToString() == roles_expected);

  auto texts = safe_cast<arrow::StringArray>(
      graph.edges.properties->GetColumnByName("text")->chunk(0));
  std::string texts_expected = std::string(
      "[\n\
  null,\n\
  null,\n\
  null,\n\
  \"stuff\",\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(texts->ToString() == texts_expected);

  // test edge types
  auto actors = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("ACTED_IN")->chunk(0));
  std::string actors_expected = std::string(
      "[\n\
  true,\n\
  true,\n\
  true,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false\n\
]");
  KATANA_ASSERT(actors->ToString() == actors_expected);

  auto directors = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("DIRECTED")->chunk(0));
  std::string directors_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  true,\n\
  false\n\
]");
  KATANA_ASSERT(directors->ToString() == directors_expected);

  auto producers = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("PRODUCED")->chunk(0));
  std::string producers_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  KATANA_ASSERT(producers->ToString() == producers_expected);

  auto partners = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("IN_SAME_MOVIE")->chunk(0));
  std::string partners_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false,\n\
  false\n\
]");
  KATANA_ASSERT(partners->ToString() == partners_expected);

  // test topology
  auto indices = graph.topology->out_indices;
  std::string indices_expected = std::string(
      "[\n\
  0,\n\
  1,\n\
  2,\n\
  4,\n\
  5,\n\
  6,\n\
  7,\n\
  8,\n\
  8\n\
]");
  KATANA_ASSERT(indices->ToString() == indices_expected);

  auto dests = graph.topology->out_dests;
  std::string dests_expected = std::string(
      "[\n\
  0,\n\
  0,\n\
  0,\n\
  7,\n\
  0,\n\
  0,\n\
  0,\n\
  0\n\
]");
  KATANA_ASSERT(dests->ToString() == dests_expected);
}

void
VerifyTypesSet(katana::GraphComponents graph) {
  KATANA_ASSERT(graph.nodes.properties->num_columns() == 5);
  KATANA_ASSERT(graph.nodes.labels->num_columns() == 4);
  KATANA_ASSERT(graph.edges.properties->num_columns() == 4);
  KATANA_ASSERT(graph.edges.labels->num_columns() == 4);

  KATANA_ASSERT(graph.nodes.properties->num_rows() == 9);
  KATANA_ASSERT(graph.nodes.labels->num_rows() == 9);
  KATANA_ASSERT(graph.edges.properties->num_rows() == 8);
  KATANA_ASSERT(graph.edges.labels->num_rows() == 8);

  KATANA_ASSERT(graph.topology->out_indices->length() == 9);
  KATANA_ASSERT(graph.topology->out_dests->length() == 8);

  // test node properties
  auto names = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("name")->chunk(0));
  std::string names_expected = std::string(
      "[\n\
  null,\n\
  \"Keanu Reeves\",\n\
  \"Carrie-Anne Moss\",\n\
  \"Laurence Fishburne\",\n\
  \"Hugo Weaving\",\n\
  \"Lilly Wachowski\",\n\
  \"Lana Wachowski\",\n\
  \"Joel Silver\",\n\
  null\n\
]");
  KATANA_ASSERT(names->ToString() == names_expected);

  auto taglines = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("tagline")->chunk(0));
  std::string taglines_expected = std::string(
      "[\n\
  \"Welcome to the Real World\",\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(taglines->ToString() == taglines_expected);

  auto titles = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("title")->chunk(0));
  std::string titles_expected = std::string(
      "[\n\
  \"The Matrix\",\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(titles->ToString() == titles_expected);

  auto released = safe_cast<arrow::Int64Array>(
      graph.nodes.properties->GetColumnByName("released")->chunk(0));
  std::string released_expected = std::string(
      "[\n\
  1999,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(released->ToString() == released_expected);

  auto borns = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("born")->chunk(0));
  std::string borns_expected = std::string(
      "[\n\
  null,\n\
  \"1964\",\n\
  \"1967\",\n\
  \"1961\",\n\
  \"1960\",\n\
  \"1967\",\n\
  \"1965\",\n\
  \"1952\",\n\
  \"1963\"\n\
]");
  KATANA_ASSERT(borns->ToString() == borns_expected);

  // test node labels
  auto movies = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("Movie")->chunk(0));
  std::string movies_expected = std::string(
      "[\n\
  true,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false\n\
]");
  KATANA_ASSERT(movies->ToString() == movies_expected);

  auto persons = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("Person")->chunk(0));
  std::string persons_expected = std::string(
      "[\n\
  false,\n\
  true,\n\
  true,\n\
  true,\n\
  true,\n\
  true,\n\
  true,\n\
  true,\n\
  true\n\
]");
  KATANA_ASSERT(persons->ToString() == persons_expected);

  auto others = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("Other")->chunk(0));
  std::string others_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  KATANA_ASSERT(others->ToString() == others_expected);

  auto randoms = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("Random")->chunk(0));
  std::string randoms_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  KATANA_ASSERT(randoms->ToString() == randoms_expected);

  // test edge properties
  auto roles = safe_cast<arrow::ListArray>(
      graph.edges.properties->GetColumnByName("roles")->chunk(0));
  std::string roles_expected = std::string(
      "[\n\
  [\n\
    \"Neo\"\n\
  ],\n\
  [\n\
    \"Trinity\",\n\
    \"more\",\n\
    \"another\"\n\
  ],\n\
  [\n\
    \"Morpheus\",\n\
    \"some stuff\",\n\
    \"test\nn\"\n\
  ],\n\
  null,\n\
  [\n\
    \"Agent Smith\",\n\
    \"alter\"\n\
  ],\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(roles->ToString() == roles_expected);

  auto numbers = safe_cast<arrow::ListArray>(
      graph.edges.properties->GetColumnByName("numbers")->chunk(0));
  std::string numbers_expected = std::string(
      "[\n\
  null,\n\
  null,\n\
  [\n\
    12,\n\
    53,\n\
    67,\n\
    32,\n\
    -1\n\
  ],\n\
  null,\n\
  [\n\
    53,\n\
    5324,\n\
    2435,\n\
    65756,\n\
    352,\n\
    3442,\n\
    2342454,\n\
    56\n\
  ],\n\
  [\n\
    2,\n\
    43,\n\
    76543\n\
  ],\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(numbers->ToString() == numbers_expected);

  auto bools = safe_cast<arrow::ListArray>(
      graph.edges.properties->GetColumnByName("bools")->chunk(0));
  std::string bools_expected = std::string(
      "[\n\
  null,\n\
  null,\n\
  [\n\
    false,\n\
    true,\n\
    false,\n\
    false\n\
  ],\n\
  null,\n\
  [\n\
    false,\n\
    false,\n\
    false,\n\
    true,\n\
    true\n\
  ],\n\
  [\n\
    false,\n\
    false\n\
  ],\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(bools->ToString() == bools_expected);

  auto texts = safe_cast<arrow::StringArray>(
      graph.edges.properties->GetColumnByName("text")->chunk(0));
  std::string texts_expected = std::string(
      "[\n\
  null,\n\
  null,\n\
  null,\n\
  \"stuff\",\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  KATANA_ASSERT(texts->ToString() == texts_expected);

  // test edge types
  auto actors = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("ACTED_IN")->chunk(0));
  std::string actors_expected = std::string(
      "[\n\
  true,\n\
  true,\n\
  true,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false\n\
]");
  KATANA_ASSERT(actors->ToString() == actors_expected);

  auto directors = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("DIRECTED")->chunk(0));
  std::string directors_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  true,\n\
  false\n\
]");
  KATANA_ASSERT(directors->ToString() == directors_expected);

  auto producers = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("PRODUCED")->chunk(0));
  std::string producers_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  KATANA_ASSERT(producers->ToString() == producers_expected);

  auto partners = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("IN_SAME_MOVIE")->chunk(0));
  std::string partners_expected = std::string(
      "[\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false,\n\
  false\n\
]");
  KATANA_ASSERT(partners->ToString() == partners_expected);

  // test topology
  auto indices = graph.topology->out_indices;
  std::string indices_expected = std::string(
      "[\n\
  0,\n\
  1,\n\
  2,\n\
  4,\n\
  5,\n\
  6,\n\
  7,\n\
  8,\n\
  8\n\
]");
  KATANA_ASSERT(indices->ToString() == indices_expected);

  auto dests = graph.topology->out_dests;
  std::string dests_expected = std::string(
      "[\n\
  0,\n\
  0,\n\
  0,\n\
  7,\n\
  0,\n\
  0,\n\
  0,\n\
  0\n\
]");
  KATANA_ASSERT(dests->ToString() == dests_expected);
}

void
VerifyChunksSet(katana::GraphComponents graph) {
  KATANA_ASSERT(graph.nodes.properties->num_columns() == 5);
  KATANA_ASSERT(graph.nodes.labels->num_columns() == 4);
  KATANA_ASSERT(graph.edges.properties->num_columns() == 4);
  KATANA_ASSERT(graph.edges.labels->num_columns() == 4);

  KATANA_ASSERT(graph.nodes.properties->num_rows() == 9);
  KATANA_ASSERT(graph.nodes.labels->num_rows() == 9);
  KATANA_ASSERT(graph.edges.properties->num_rows() == 8);
  KATANA_ASSERT(graph.edges.labels->num_rows() == 8);

  KATANA_ASSERT(graph.topology->out_indices->length() == 9);
  KATANA_ASSERT(graph.topology->out_dests->length() == 8);

  // test node properties
  auto names = graph.nodes.properties->GetColumnByName("name");
  std::string names_expected = std::string(
      "[\n\
  [\n\
    null,\n\
    \"Keanu Reeves\",\n\
    \"Carrie-Anne Moss\"\n\
  ],\n\
  [\n\
    \"Laurence Fishburne\",\n\
    \"Hugo Weaving\",\n\
    \"Lilly Wachowski\"\n\
  ],\n\
  [\n\
    \"Lana Wachowski\",\n\
    \"Joel Silver\",\n\
    null\n\
  ]\n\
]");
  KATANA_ASSERT(names->ToString() == names_expected);

  auto taglines = graph.nodes.properties->GetColumnByName("tagline");
  std::string taglines_expected = std::string(
      "[\n\
  [\n\
    \"Welcome to the Real World\",\n\
    null,\n\
    null\n\
  ],\n\
  [\n\
    null,\n\
    null,\n\
    null\n\
  ],\n\
  [\n\
    null,\n\
    null,\n\
    null\n\
  ]\n\
]");
  KATANA_ASSERT(taglines->ToString() == taglines_expected);

  auto titles = graph.nodes.properties->GetColumnByName("title");
  std::string titles_expected = std::string(
      "[\n\
  [\n\
    \"The Matrix\",\n\
    null,\n\
    null\n\
  ],\n\
  [\n\
    null,\n\
    null,\n\
    null\n\
  ],\n\
  [\n\
    null,\n\
    null,\n\
    null\n\
  ]\n\
]");
  KATANA_ASSERT(titles->ToString() == titles_expected);

  auto released = graph.nodes.properties->GetColumnByName("released");
  std::string released_expected = std::string(
      "[\n\
  [\n\
    1999,\n\
    null,\n\
    null\n\
  ],\n\
  [\n\
    null,\n\
    null,\n\
    null\n\
  ],\n\
  [\n\
    null,\n\
    null,\n\
    null\n\
  ]\n\
]");
  KATANA_ASSERT(released->ToString() == released_expected);

  auto borns = graph.nodes.properties->GetColumnByName("born");
  std::string borns_expected = std::string(
      "[\n\
  [\n\
    null,\n\
    \"1964\",\n\
    \"1967\"\n\
  ],\n\
  [\n\
    \"1961\",\n\
    \"1960\",\n\
    \"1967\"\n\
  ],\n\
  [\n\
    \"1965\",\n\
    \"1952\",\n\
    \"1963\"\n\
  ]\n\
]");
  KATANA_ASSERT(borns->ToString() == borns_expected);

  // test node labels
  auto movies = graph.nodes.labels->GetColumnByName("Movie");
  std::string movies_expected = std::string(
      "[\n\
  [\n\
    true,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ]\n\
]");
  KATANA_ASSERT(movies->ToString() == movies_expected);

  auto persons = graph.nodes.labels->GetColumnByName("Person");
  std::string persons_expected = std::string(
      "[\n\
  [\n\
    false,\n\
    true,\n\
    true\n\
  ],\n\
  [\n\
    true,\n\
    true,\n\
    true\n\
  ],\n\
  [\n\
    true,\n\
    true,\n\
    true\n\
  ]\n\
]");
  KATANA_ASSERT(persons->ToString() == persons_expected);

  auto others = graph.nodes.labels->GetColumnByName("Other");
  std::string others_expected = std::string(
      "[\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false,\n\
    true\n\
  ]\n\
]");
  KATANA_ASSERT(others->ToString() == others_expected);

  auto randoms = graph.nodes.labels->GetColumnByName("Random");
  std::string randoms_expected = std::string(
      "[\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false,\n\
    true\n\
  ]\n\
]");
  KATANA_ASSERT(randoms->ToString() == randoms_expected);

  // test edge properties
  auto roles = graph.edges.properties->GetColumnByName("roles");
  std::string roles_expected = std::string(
      "[\n\
  [\n\
    [\n\
      \"Neo\"\n\
    ],\n\
    [\n\
      \"Trinity\",\n\
      \"more\",\n\
      \"another\"\n\
    ],\n\
    [\n\
      \"Morpheus\",\n\
      \"some stuff\",\n\
      \"test\nn\"\n\
    ]\n\
  ],\n\
  [\n\
    null,\n\
    [\n\
      \"Agent Smith\",\n\
      \"alter\"\n\
    ],\n\
    null\n\
  ],\n\
  [\n\
    null,\n\
    null\n\
  ]\n\
]");
  KATANA_ASSERT(roles->ToString() == roles_expected);

  auto numbers = graph.edges.properties->GetColumnByName("numbers");
  std::string numbers_expected = std::string(
      "[\n\
  [\n\
    null,\n\
    null,\n\
    [\n\
      12,\n\
      53,\n\
      67,\n\
      32,\n\
      -1\n\
    ]\n\
  ],\n\
  [\n\
    null,\n\
    [\n\
      53,\n\
      5324,\n\
      2435,\n\
      65756,\n\
      352,\n\
      3442,\n\
      2342454,\n\
      56\n\
    ],\n\
    [\n\
      2,\n\
      43,\n\
      76543\n\
    ]\n\
  ],\n\
  [\n\
    null,\n\
    null\n\
  ]\n\
]");
  KATANA_ASSERT(numbers->ToString() == numbers_expected);

  auto bools = graph.edges.properties->GetColumnByName("bools");
  std::string bools_expected = std::string(
      "[\n\
  [\n\
    null,\n\
    null,\n\
    [\n\
      false,\n\
      true,\n\
      false,\n\
      false\n\
    ]\n\
  ],\n\
  [\n\
    null,\n\
    [\n\
      false,\n\
      false,\n\
      false,\n\
      true,\n\
      true\n\
    ],\n\
    [\n\
      false,\n\
      false\n\
    ]\n\
  ],\n\
  [\n\
    null,\n\
    null\n\
  ]\n\
]");
  KATANA_ASSERT(bools->ToString() == bools_expected);

  auto texts = graph.edges.properties->GetColumnByName("text");
  std::string texts_expected = std::string(
      "[\n\
  [\n\
    null,\n\
    null,\n\
    null\n\
  ],\n\
  [\n\
    \"stuff\",\n\
    null,\n\
    null\n\
  ],\n\
  [\n\
    null,\n\
    null\n\
  ]\n\
]");
  KATANA_ASSERT(texts->ToString() == texts_expected);

  // test edge types
  auto actors = graph.edges.labels->GetColumnByName("ACTED_IN");
  std::string actors_expected = std::string(
      "[\n\
  [\n\
    true,\n\
    true,\n\
    true\n\
  ],\n\
  [\n\
    false,\n\
    true,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false\n\
  ]\n\
]");
  KATANA_ASSERT(actors->ToString() == actors_expected);

  auto directors = graph.edges.labels->GetColumnByName("DIRECTED");
  std::string directors_expected = std::string(
      "[\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false,\n\
    true\n\
  ],\n\
  [\n\
    true,\n\
    false\n\
  ]\n\
]");
  KATANA_ASSERT(directors->ToString() == directors_expected);

  auto producers = graph.edges.labels->GetColumnByName("PRODUCED");
  std::string producers_expected = std::string(
      "[\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    true\n\
  ]\n\
]");
  KATANA_ASSERT(producers->ToString() == producers_expected);

  auto partners = graph.edges.labels->GetColumnByName("IN_SAME_MOVIE");
  std::string partners_expected = std::string(
      "[\n\
  [\n\
    false,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    true,\n\
    false,\n\
    false\n\
  ],\n\
  [\n\
    false,\n\
    false\n\
  ]\n\
]");
  KATANA_ASSERT(partners->ToString() == partners_expected);

  // test topology
  auto indices = graph.topology->out_indices;
  std::string indices_expected = std::string(
      "[\n\
  0,\n\
  1,\n\
  2,\n\
  4,\n\
  5,\n\
  6,\n\
  7,\n\
  8,\n\
  8\n\
]");
  KATANA_ASSERT(indices->ToString() == indices_expected);

  auto dests = graph.topology->out_dests;
  std::string dests_expected = std::string(
      "[\n\
  0,\n\
  0,\n\
  0,\n\
  7,\n\
  0,\n\
  0,\n\
  0,\n\
  0\n\
]");
  KATANA_ASSERT(dests->ToString() == dests_expected);
}

#if defined(KATANA_MONGOC_FOUND)
katana::GraphComponents
GenerateAndConvertBson(size_t chunk_size) {
  katana::PropertyGraphBuilder builder{chunk_size};

  bson_oid_t george_oid;
  bson_oid_init_from_string(&george_oid, "5efca3f859a16711627b03f7");
  bson_oid_t frank_oid;
  bson_oid_init_from_string(&frank_oid, "5efca3f859a16711627b03f8");
  bson_oid_t friend_oid;
  bson_oid_init_from_string(&friend_oid, "5efca3f859a16711627b03f9");

  bson_t* george = BCON_NEW(
      "_id", BCON_OID(&george_oid), "name", BCON_UTF8("George"), "born",
      BCON_DOUBLE(1985));
  katana::HandleNodeDocumentMongoDB(&builder, george, "person");
  bson_destroy(george);
  bson_t* frank = BCON_NEW(
      "_id", BCON_OID(&frank_oid), "name", BCON_UTF8("Frank"), "born",
      BCON_DOUBLE(1989));
  katana::HandleNodeDocumentMongoDB(&builder, frank, "person");
  bson_destroy(frank);

  bson_t* friend_doc = BCON_NEW(
      "_id", BCON_OID(&friend_oid), "friend1", BCON_OID(&george_oid), "friend2",
      BCON_OID(&frank_oid), "met", BCON_DOUBLE(2000));
  katana::HandleEdgeDocumentMongoDB(&builder, friend_doc, "friend");
  bson_destroy(friend_doc);

  return builder.Finish();
}
#endif

#if defined(KATANA_MONGOC_FOUND)
void
VerifyMongodbSet(const katana::GraphComponents& graph) {
  KATANA_ASSERT(graph.nodes.properties->num_columns() == 2);
  KATANA_ASSERT(graph.nodes.labels->num_columns() == 1);
  KATANA_ASSERT(graph.edges.properties->num_columns() == 1);
  KATANA_ASSERT(graph.edges.labels->num_columns() == 1);

  KATANA_ASSERT(graph.nodes.properties->num_rows() == 2);
  KATANA_ASSERT(graph.nodes.labels->num_rows() == 2);
  KATANA_ASSERT(graph.edges.properties->num_rows() == 1);
  KATANA_ASSERT(graph.edges.labels->num_rows() == 1);

  KATANA_ASSERT(graph.topology->out_indices->length() == 2);
  KATANA_ASSERT(graph.topology->out_dests->length() == 1);

  // test node properties
  auto names = safe_cast<arrow::StringArray>(
      graph.nodes.properties->GetColumnByName("name")->chunk(0));
  std::string names_expected = std::string(
      "[\n\
  \"George\",\n\
  \"Frank\"\n\
]");
  KATANA_ASSERT(names->ToString() == names_expected);

  auto born = safe_cast<arrow::DoubleArray>(
      graph.nodes.properties->GetColumnByName("born")->chunk(0));
  std::string born_expected = std::string(
      "[\n\
  1985,\n\
  1989\n\
]");
  KATANA_ASSERT(born->ToString() == born_expected);

  // test node labels
  auto people = safe_cast<arrow::BooleanArray>(
      graph.nodes.labels->GetColumnByName("person")->chunk(0));
  std::string people_expected = std::string(
      "[\n\
  true,\n\
  true\n\
]");
  KATANA_ASSERT(people->ToString() == people_expected);

  // test edge properties
  auto mets = safe_cast<arrow::DoubleArray>(
      graph.edges.properties->GetColumnByName("met")->chunk(0));
  std::string mets_expected = std::string(
      "[\n\
  2000\n\
]");
  KATANA_ASSERT(mets->ToString() == mets_expected);

  // test edge labels
  auto friends = safe_cast<arrow::BooleanArray>(
      graph.edges.labels->GetColumnByName("friend")->chunk(0));
  std::string friends_expected = std::string(
      "[\n\
  true\n\
]");
  KATANA_ASSERT(friends->ToString() == friends_expected);

  // test topology
  auto indices = graph.topology->out_indices;
  std::string indices_expected = std::string(
      "[\n\
  1,\n\
  1\n\
]");
  KATANA_ASSERT(indices->ToString() == indices_expected);

  auto dests = graph.topology->out_dests;
  std::string dests_expected = std::string(
      "[\n\
  1\n\
]");
  KATANA_ASSERT(dests->ToString() == dests_expected);
}
#endif

}  // namespace

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  katana::GraphComponents graph;

  switch (fileType) {
  case katana::SourceDatabase::kNeo4j:
    graph = katana::ConvertGraphML(input_filename, chunk_size);
    break;
#if defined(KATANA_MONGOC_FOUND)
  case katana::SourceDatabase::kMongodb:
    graph = GenerateAndConvertBson(chunk_size);
    break;
#endif
  default:
    KATANA_LOG_FATAL("unknown option {}", fileType);
  }

  switch (test_type) {
  case ConvertTest::kMovies:
    VerifyMovieSet(graph);
    break;
  case ConvertTest::kTypes:
    VerifyTypesSet(graph);
    break;
  case ConvertTest::kChunks:
    VerifyChunksSet(graph);
    break;
#if defined(KATANA_MONGOC_FOUND)
  case ConvertTest::kMongodb:
    VerifyMongodbSet(graph);
    break;
#endif
  default:
    KATANA_LOG_FATAL("unknown option {}", test_type);
  }

  return 0;
}

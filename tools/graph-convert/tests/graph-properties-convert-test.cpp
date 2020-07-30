#include "galois/Logging.h"
#include "graph-properties-convert-graphml.h"
#include "graph-properties-convert-mongodb.h"

#include <llvm/Support/CommandLine.h>
#include <memory>

#include "galois/Galois.h"

namespace {
enum ConvertTest { kMovies, kTypes, kChunks };
}

namespace cll = llvm::cl;

static cll::opt<std::string> input_filename(cll::Positional,
                                            cll::desc("<input file/directory>"),
                                            cll::Required);
static cll::opt<galois::SourceType>
    fileType(cll::desc("Input file type:"),
             cll::values(clEnumValN(galois::SourceType::kGraphml, "graphml",
                                    "source file is of type GraphML")),
             cll::Required);
static cll::opt<ConvertTest> test_type(
    cll::desc("Input file type:"),
    cll::values(clEnumValN(ConvertTest::kTypes, "types",
                           "source file is a test for graphml type conversion"),
                clEnumValN(ConvertTest::kMovies, "movies",
                           "source file is a test for generic conversion"),
                clEnumValN(ConvertTest::kChunks, "chunks",
                           "this is a test for chunks")),
    cll::Required);
static cll::opt<int>
    chunk_size("chunkSize",
               cll::desc("Chunk size for in memory arrow representation"),
               cll::init(25000));

namespace {

template <typename T, typename U>
std::shared_ptr<T> safe_cast(const std::shared_ptr<U>& r) noexcept {
  auto p = std::dynamic_pointer_cast<T>(r);
  GALOIS_LOG_ASSERT(p);
  return p;
}

void VerifyMovieSet(const galois::GraphComponents& graph) {
  GALOIS_ASSERT(graph.node_properties->num_columns() == 5);
  GALOIS_ASSERT(graph.node_labels->num_columns() == 4);
  GALOIS_ASSERT(graph.edge_properties->num_columns() == 2);
  GALOIS_ASSERT(graph.edge_types->num_columns() == 4);

  GALOIS_ASSERT(graph.node_properties->num_rows() == 9);
  GALOIS_ASSERT(graph.node_labels->num_rows() == 9);
  GALOIS_ASSERT(graph.edge_properties->num_rows() == 8);
  GALOIS_ASSERT(graph.edge_types->num_rows() == 8);

  GALOIS_ASSERT(graph.topology->out_indices->length() == 9);
  GALOIS_ASSERT(graph.topology->out_dests->length() == 8);

  // test node properties
  auto names = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("name")->chunk(0));
  std::string names_expected = std::string("[\n\
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
  GALOIS_ASSERT(names->ToString() == names_expected);

  auto taglines = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("tagline")->chunk(0));
  std::string taglines_expected = std::string("[\n\
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
  GALOIS_ASSERT(taglines->ToString() == taglines_expected);

  auto titles = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("title")->chunk(0));
  std::string titles_expected = std::string("[\n\
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
  GALOIS_ASSERT(titles->ToString() == titles_expected);

  auto released = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("released")->chunk(0));
  std::string released_expected = std::string("[\n\
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
  GALOIS_ASSERT(released->ToString() == released_expected);

  auto borns = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("born")->chunk(0));
  std::string borns_expected = std::string("[\n\
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
  GALOIS_ASSERT(borns->ToString() == borns_expected);

  // test node labels
  auto movies = safe_cast<arrow::BooleanArray>(
      graph.node_labels->GetColumnByName("Movie")->chunk(0));
  std::string movies_expected = std::string("[\n\
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
  GALOIS_ASSERT(movies->ToString() == movies_expected);

  auto persons = safe_cast<arrow::BooleanArray>(
      graph.node_labels->GetColumnByName("Person")->chunk(0));
  std::string persons_expected = std::string("[\n\
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
  GALOIS_ASSERT(persons->ToString() == persons_expected);

  auto others = safe_cast<arrow::BooleanArray>(
      graph.node_labels->GetColumnByName("Other")->chunk(0));
  std::string others_expected = std::string("[\n\
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
  GALOIS_ASSERT(others->ToString() == others_expected);

  auto randoms = safe_cast<arrow::BooleanArray>(
      graph.node_labels->GetColumnByName("Random")->chunk(0));
  std::string randoms_expected = std::string("[\n\
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
  GALOIS_ASSERT(randoms->ToString() == randoms_expected);

  // test edge properties
  auto roles = safe_cast<arrow::StringArray>(
      graph.edge_properties->GetColumnByName("roles")->chunk(0));
  std::string roles_expected = std::string("[\n\
  \"Neo\",\n\
  \"Trinity\",\n\
  \"Morpheus\",\n\
  null,\n\
  \"Agent Smith\",\n\
  null,\n\
  null,\n\
  null\n\
]");
  GALOIS_ASSERT(roles->ToString() == roles_expected);

  auto texts = safe_cast<arrow::StringArray>(
      graph.edge_properties->GetColumnByName("text")->chunk(0));
  std::string texts_expected = std::string("[\n\
  null,\n\
  null,\n\
  null,\n\
  \"stuff\",\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  GALOIS_ASSERT(texts->ToString() == texts_expected);

  // test edge types
  auto actors = safe_cast<arrow::BooleanArray>(
      graph.edge_types->GetColumnByName("ACTED_IN")->chunk(0));
  std::string actors_expected = std::string("[\n\
  true,\n\
  true,\n\
  true,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false\n\
]");
  GALOIS_ASSERT(actors->ToString() == actors_expected);

  auto directors = safe_cast<arrow::BooleanArray>(
      graph.edge_types->GetColumnByName("DIRECTED")->chunk(0));
  std::string directors_expected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  true,\n\
  false\n\
]");
  GALOIS_ASSERT(directors->ToString() == directors_expected);

  auto producers = safe_cast<arrow::BooleanArray>(
      graph.edge_types->GetColumnByName("PRODUCED")->chunk(0));
  std::string producers_expected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  GALOIS_ASSERT(producers->ToString() == producers_expected);

  auto partners = safe_cast<arrow::BooleanArray>(
      graph.edge_types->GetColumnByName("IN_SAME_MOVIE")->chunk(0));
  std::string partners_expected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false,\n\
  false\n\
]");
  GALOIS_ASSERT(partners->ToString() == partners_expected);

  // test topology
  auto indices                 = graph.topology->out_indices;
  std::string indices_expected = std::string("[\n\
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
  GALOIS_ASSERT(indices->ToString() == indices_expected);

  auto dests                 = graph.topology->out_dests;
  std::string dests_expected = std::string("[\n\
  0,\n\
  0,\n\
  0,\n\
  7,\n\
  0,\n\
  0,\n\
  0,\n\
  0\n\
]");
  GALOIS_ASSERT(dests->ToString() == dests_expected);
}

void VerifyTypesSet(galois::GraphComponents graph) {
  GALOIS_ASSERT(graph.node_properties->num_columns() == 5);
  GALOIS_ASSERT(graph.node_labels->num_columns() == 4);
  GALOIS_ASSERT(graph.edge_properties->num_columns() == 4);
  GALOIS_ASSERT(graph.edge_types->num_columns() == 4);

  GALOIS_ASSERT(graph.node_properties->num_rows() == 9);
  GALOIS_ASSERT(graph.node_labels->num_rows() == 9);
  GALOIS_ASSERT(graph.edge_properties->num_rows() == 8);
  GALOIS_ASSERT(graph.edge_types->num_rows() == 8);

  GALOIS_ASSERT(graph.topology->out_indices->length() == 9);
  GALOIS_ASSERT(graph.topology->out_dests->length() == 8);

  // test node properties
  auto names = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("name")->chunk(0));
  std::string names_expected = std::string("[\n\
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
  GALOIS_ASSERT(names->ToString() == names_expected);

  auto taglines = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("tagline")->chunk(0));
  std::string taglines_expected = std::string("[\n\
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
  GALOIS_ASSERT(taglines->ToString() == taglines_expected);

  auto titles = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("title")->chunk(0));
  std::string titles_expected = std::string("[\n\
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
  GALOIS_ASSERT(titles->ToString() == titles_expected);

  auto released = safe_cast<arrow::Int64Array>(
      graph.node_properties->GetColumnByName("released")->chunk(0));
  std::string released_expected = std::string("[\n\
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
  GALOIS_ASSERT(released->ToString() == released_expected);

  auto borns = safe_cast<arrow::StringArray>(
      graph.node_properties->GetColumnByName("born")->chunk(0));
  std::string borns_expected = std::string("[\n\
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
  GALOIS_ASSERT(borns->ToString() == borns_expected);

  // test node labels
  auto movies = safe_cast<arrow::BooleanArray>(
      graph.node_labels->GetColumnByName("Movie")->chunk(0));
  std::string movies_expected = std::string("[\n\
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
  GALOIS_ASSERT(movies->ToString() == movies_expected);

  auto persons = safe_cast<arrow::BooleanArray>(
      graph.node_labels->GetColumnByName("Person")->chunk(0));
  std::string persons_expected = std::string("[\n\
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
  GALOIS_ASSERT(persons->ToString() == persons_expected);

  auto others = safe_cast<arrow::BooleanArray>(
      graph.node_labels->GetColumnByName("Other")->chunk(0));
  std::string others_expected = std::string("[\n\
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
  GALOIS_ASSERT(others->ToString() == others_expected);

  auto randoms = safe_cast<arrow::BooleanArray>(
      graph.node_labels->GetColumnByName("Random")->chunk(0));
  std::string randoms_expected = std::string("[\n\
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
  GALOIS_ASSERT(randoms->ToString() == randoms_expected);

  // test edge properties
  auto roles = safe_cast<arrow::ListArray>(
      graph.edge_properties->GetColumnByName("roles")->chunk(0));
  std::string roles_expected = std::string("[\n\
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
  GALOIS_ASSERT(roles->ToString() == roles_expected);

  auto numbers = safe_cast<arrow::ListArray>(
      graph.edge_properties->GetColumnByName("numbers")->chunk(0));
  std::string numbers_expected = std::string("[\n\
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
  GALOIS_ASSERT(numbers->ToString() == numbers_expected);

  auto bools = safe_cast<arrow::ListArray>(
      graph.edge_properties->GetColumnByName("bools")->chunk(0));
  std::string bools_expected = std::string("[\n\
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
  GALOIS_ASSERT(bools->ToString() == bools_expected);

  auto texts = safe_cast<arrow::StringArray>(
      graph.edge_properties->GetColumnByName("text")->chunk(0));
  std::string texts_expected = std::string("[\n\
  null,\n\
  null,\n\
  null,\n\
  \"stuff\",\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  GALOIS_ASSERT(texts->ToString() == texts_expected);

  // test edge types
  auto actors = safe_cast<arrow::BooleanArray>(
      graph.edge_types->GetColumnByName("ACTED_IN")->chunk(0));
  std::string actors_expected = std::string("[\n\
  true,\n\
  true,\n\
  true,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false\n\
]");
  GALOIS_ASSERT(actors->ToString() == actors_expected);

  auto directors = safe_cast<arrow::BooleanArray>(
      graph.edge_types->GetColumnByName("DIRECTED")->chunk(0));
  std::string directors_expected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  true,\n\
  false\n\
]");
  GALOIS_ASSERT(directors->ToString() == directors_expected);

  auto producers = safe_cast<arrow::BooleanArray>(
      graph.edge_types->GetColumnByName("PRODUCED")->chunk(0));
  std::string producers_expected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  GALOIS_ASSERT(producers->ToString() == producers_expected);

  auto partners = safe_cast<arrow::BooleanArray>(
      graph.edge_types->GetColumnByName("IN_SAME_MOVIE")->chunk(0));
  std::string partners_expected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false,\n\
  false\n\
]");
  GALOIS_ASSERT(partners->ToString() == partners_expected);

  // test topology
  auto indices                 = graph.topology->out_indices;
  std::string indices_expected = std::string("[\n\
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
  GALOIS_ASSERT(indices->ToString() == indices_expected);

  auto dests                 = graph.topology->out_dests;
  std::string dests_expected = std::string("[\n\
  0,\n\
  0,\n\
  0,\n\
  7,\n\
  0,\n\
  0,\n\
  0,\n\
  0\n\
]");
  GALOIS_ASSERT(dests->ToString() == dests_expected);
}

void VerifyChunksSet(galois::GraphComponents graph) {
  GALOIS_ASSERT(graph.node_properties->num_columns() == 5);
  GALOIS_ASSERT(graph.node_labels->num_columns() == 4);
  GALOIS_ASSERT(graph.edge_properties->num_columns() == 4);
  GALOIS_ASSERT(graph.edge_types->num_columns() == 4);

  GALOIS_ASSERT(graph.node_properties->num_rows() == 9);
  GALOIS_ASSERT(graph.node_labels->num_rows() == 9);
  GALOIS_ASSERT(graph.edge_properties->num_rows() == 8);
  GALOIS_ASSERT(graph.edge_types->num_rows() == 8);

  GALOIS_ASSERT(graph.topology->out_indices->length() == 9);
  GALOIS_ASSERT(graph.topology->out_dests->length() == 8);

  // test node properties
  auto names                 = graph.node_properties->GetColumnByName("name");
  std::string names_expected = std::string("[\n\
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
  GALOIS_ASSERT(names->ToString() == names_expected);

  auto taglines = graph.node_properties->GetColumnByName("tagline");
  std::string taglines_expected = std::string("[\n\
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
  GALOIS_ASSERT(taglines->ToString() == taglines_expected);

  auto titles                 = graph.node_properties->GetColumnByName("title");
  std::string titles_expected = std::string("[\n\
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
  GALOIS_ASSERT(titles->ToString() == titles_expected);

  auto released = graph.node_properties->GetColumnByName("released");
  std::string released_expected = std::string("[\n\
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
  GALOIS_ASSERT(released->ToString() == released_expected);

  auto borns                 = graph.node_properties->GetColumnByName("born");
  std::string borns_expected = std::string("[\n\
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
  GALOIS_ASSERT(borns->ToString() == borns_expected);

  // test node labels
  auto movies                 = graph.node_labels->GetColumnByName("Movie");
  std::string movies_expected = std::string("[\n\
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
  GALOIS_ASSERT(movies->ToString() == movies_expected);

  auto persons                 = graph.node_labels->GetColumnByName("Person");
  std::string persons_expected = std::string("[\n\
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
  GALOIS_ASSERT(persons->ToString() == persons_expected);

  auto others                 = graph.node_labels->GetColumnByName("Other");
  std::string others_expected = std::string("[\n\
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
  GALOIS_ASSERT(others->ToString() == others_expected);

  auto randoms                 = graph.node_labels->GetColumnByName("Random");
  std::string randoms_expected = std::string("[\n\
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
  GALOIS_ASSERT(randoms->ToString() == randoms_expected);

  // test edge properties
  auto roles                 = graph.edge_properties->GetColumnByName("roles");
  std::string roles_expected = std::string("[\n\
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
  GALOIS_ASSERT(roles->ToString() == roles_expected);

  auto numbers = graph.edge_properties->GetColumnByName("numbers");
  std::string numbers_expected = std::string("[\n\
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
  GALOIS_ASSERT(numbers->ToString() == numbers_expected);

  auto bools                 = graph.edge_properties->GetColumnByName("bools");
  std::string bools_expected = std::string("[\n\
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
  GALOIS_ASSERT(bools->ToString() == bools_expected);

  auto texts                 = graph.edge_properties->GetColumnByName("text");
  std::string texts_expected = std::string("[\n\
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
  GALOIS_ASSERT(texts->ToString() == texts_expected);

  // test edge types
  auto actors                 = graph.edge_types->GetColumnByName("ACTED_IN");
  std::string actors_expected = std::string("[\n\
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
  GALOIS_ASSERT(actors->ToString() == actors_expected);

  auto directors = graph.edge_types->GetColumnByName("DIRECTED");
  std::string directors_expected = std::string("[\n\
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
  GALOIS_ASSERT(directors->ToString() == directors_expected);

  auto producers = graph.edge_types->GetColumnByName("PRODUCED");
  std::string producers_expected = std::string("[\n\
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
  GALOIS_ASSERT(producers->ToString() == producers_expected);

  auto partners = graph.edge_types->GetColumnByName("IN_SAME_MOVIE");
  std::string partners_expected = std::string("[\n\
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
  GALOIS_ASSERT(partners->ToString() == partners_expected);

  // test topology
  auto indices                 = graph.topology->out_indices;
  std::string indices_expected = std::string("[\n\
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
  GALOIS_ASSERT(indices->ToString() == indices_expected);

  auto dests                 = graph.topology->out_dests;
  std::string dests_expected = std::string("[\n\
  0,\n\
  0,\n\
  0,\n\
  7,\n\
  0,\n\
  0,\n\
  0,\n\
  0\n\
]");
  GALOIS_ASSERT(dests->ToString() == dests_expected);
}

} // namespace

int main(int argc, char** argv) {
  galois::SharedMemSys sys;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  galois::GraphComponents graph;

  switch (fileType) {
  case galois::SourceType::kGraphml:
    graph = galois::ConvertGraphML(input_filename, chunk_size);
    break;
  default:
    GALOIS_LOG_FATAL("unknown option {}", fileType);
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
  }

  return 0;
}

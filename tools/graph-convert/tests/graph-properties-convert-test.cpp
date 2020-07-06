#include "galois/Logging.h"
#include "graph-properties-convert.h"

#include <llvm/Support/CommandLine.h>
#include <memory>

#include "galois/Galois.h"

namespace {
enum ConvertTest { MOVIES, TYPES };
}

namespace cll = llvm::cl;

static cll::opt<std::string> inputFilename(cll::Positional,
                                           cll::desc("<input file/directory>"),
                                           cll::Required);
static cll::opt<galois::SourceType>
    fileType(cll::desc("Input file type:"),
             cll::values(clEnumValN(galois::SourceType::GRAPHML, "graphml",
                                    "source file is of type GraphML"),
                         clEnumValN(galois::SourceType::JSON, "json",
                                    "source file is of type JSON"),
                         clEnumValN(galois::SourceType::CSV, "csv",
                                    "source file is of type CSV")),
             cll::Required);
static cll::opt<ConvertTest> testType(
    cll::desc("Input file type:"),
    cll::values(clEnumValN(ConvertTest::TYPES, "types",
                           "source file is a test for graphml type conversion"),
                clEnumValN(ConvertTest::MOVIES, "movies",
                           "source file is a test for generic conversion")),
    cll::Required);

namespace {

template <typename T, typename U>
std::shared_ptr<T> safe_cast(const std::shared_ptr<U>& r) noexcept {
  auto p = std::dynamic_pointer_cast<T>(r);
  GALOIS_LOG_ASSERT(p);
  return p;
}

void verifyMovieSet(const galois::GraphComponents& graph) {
  GALOIS_ASSERT(graph.nodeProperties->num_columns() == 5);
  GALOIS_ASSERT(graph.nodeLabels->num_columns() == 4);
  GALOIS_ASSERT(graph.edgeProperties->num_columns() == 2);
  GALOIS_ASSERT(graph.edgeTypes->num_columns() == 4);

  GALOIS_ASSERT(graph.nodeProperties->num_rows() == 9);
  GALOIS_ASSERT(graph.nodeLabels->num_rows() == 9);
  GALOIS_ASSERT(graph.edgeProperties->num_rows() == 8);
  GALOIS_ASSERT(graph.edgeTypes->num_rows() == 8);

  GALOIS_ASSERT(graph.topology->out_indices->length() == 9);
  GALOIS_ASSERT(graph.topology->out_dests->length() == 8);

  // test node properties
  auto names = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("name")->chunk(0));
  std::string namesExpected = std::string("[\n\
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
  GALOIS_ASSERT(names->ToString() == namesExpected);

  auto taglines = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("tagline")->chunk(0));
  std::string taglinesExpected = std::string("[\n\
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
  GALOIS_ASSERT(taglines->ToString() == taglinesExpected);

  auto titles = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("title")->chunk(0));
  std::string titlesExpected = std::string("[\n\
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
  GALOIS_ASSERT(titles->ToString() == titlesExpected);

  auto released = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("released")->chunk(0));
  std::string releasedExpected = std::string("[\n\
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
  GALOIS_ASSERT(released->ToString() == releasedExpected);

  auto borns = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("born")->chunk(0));
  std::string bornsExpected = std::string("[\n\
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
  GALOIS_ASSERT(borns->ToString() == bornsExpected);

  // test node labels
  auto movies = safe_cast<arrow::BooleanArray>(
      graph.nodeLabels->GetColumnByName("Movie")->chunk(0));
  std::string moviesExpected = std::string("[\n\
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
  GALOIS_ASSERT(movies->ToString() == moviesExpected);

  auto persons = safe_cast<arrow::BooleanArray>(
      graph.nodeLabels->GetColumnByName("Person")->chunk(0));
  std::string personsExpected = std::string("[\n\
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
  GALOIS_ASSERT(persons->ToString() == personsExpected);

  auto others = safe_cast<arrow::BooleanArray>(
      graph.nodeLabels->GetColumnByName("Other")->chunk(0));
  std::string othersExpected = std::string("[\n\
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
  GALOIS_ASSERT(others->ToString() == othersExpected);

  auto randoms = safe_cast<arrow::BooleanArray>(
      graph.nodeLabels->GetColumnByName("Random")->chunk(0));
  std::string randomsExpected = std::string("[\n\
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
  GALOIS_ASSERT(randoms->ToString() == randomsExpected);

  // test edge properties
  auto roles = safe_cast<arrow::StringArray>(
      graph.edgeProperties->GetColumnByName("roles")->chunk(0));
  std::string rolesExpected = std::string("[\n\
  \"Neo\",\n\
  \"Trinity\",\n\
  \"Morpheus\",\n\
  null,\n\
  \"Agent Smith\",\n\
  null,\n\
  null,\n\
  null\n\
]");
  GALOIS_ASSERT(roles->ToString() == rolesExpected);

  auto texts = safe_cast<arrow::StringArray>(
      graph.edgeProperties->GetColumnByName("text")->chunk(0));
  std::string textsExpected = std::string("[\n\
  null,\n\
  null,\n\
  null,\n\
  \"stuff\",\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  GALOIS_ASSERT(texts->ToString() == textsExpected);

  // test edge types
  auto actors = safe_cast<arrow::BooleanArray>(
      graph.edgeTypes->GetColumnByName("ACTED_IN")->chunk(0));
  std::string actorsExpected = std::string("[\n\
  true,\n\
  true,\n\
  true,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false\n\
]");
  GALOIS_ASSERT(actors->ToString() == actorsExpected);

  auto directors = safe_cast<arrow::BooleanArray>(
      graph.edgeTypes->GetColumnByName("DIRECTED")->chunk(0));
  std::string directorsExpected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  true,\n\
  false\n\
]");
  GALOIS_ASSERT(directors->ToString() == directorsExpected);

  auto producers = safe_cast<arrow::BooleanArray>(
      graph.edgeTypes->GetColumnByName("PRODUCED")->chunk(0));
  std::string producersExpected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  GALOIS_ASSERT(producers->ToString() == producersExpected);

  auto partners = safe_cast<arrow::BooleanArray>(
      graph.edgeTypes->GetColumnByName("IN_SAME_MOVIE")->chunk(0));
  std::string partnersExpected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false,\n\
  false\n\
]");
  GALOIS_ASSERT(partners->ToString() == partnersExpected);

  // test topology
  auto indices                = graph.topology->out_indices;
  std::string indicesExpected = std::string("[\n\
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
  GALOIS_ASSERT(indices->ToString() == indicesExpected);

  auto dests                = graph.topology->out_dests;
  std::string destsExpected = std::string("[\n\
  0,\n\
  0,\n\
  0,\n\
  7,\n\
  0,\n\
  0,\n\
  0,\n\
  0\n\
]");
  GALOIS_ASSERT(dests->ToString() == destsExpected);
}

void verifyTypesSet(galois::GraphComponents graph) {
  GALOIS_ASSERT(graph.nodeProperties->num_columns() == 5);
  GALOIS_ASSERT(graph.nodeLabels->num_columns() == 4);
  GALOIS_ASSERT(graph.edgeProperties->num_columns() == 4);
  GALOIS_ASSERT(graph.edgeTypes->num_columns() == 4);

  GALOIS_ASSERT(graph.nodeProperties->num_rows() == 9);
  GALOIS_ASSERT(graph.nodeLabels->num_rows() == 9);
  GALOIS_ASSERT(graph.edgeProperties->num_rows() == 8);
  GALOIS_ASSERT(graph.edgeTypes->num_rows() == 8);

  GALOIS_ASSERT(graph.topology->out_indices->length() == 9);
  GALOIS_ASSERT(graph.topology->out_dests->length() == 8);

  // test node properties
  auto names = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("name")->chunk(0));
  std::string namesExpected = std::string("[\n\
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
  GALOIS_ASSERT(names->ToString() == namesExpected);

  auto taglines = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("tagline")->chunk(0));
  std::string taglinesExpected = std::string("[\n\
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
  GALOIS_ASSERT(taglines->ToString() == taglinesExpected);

  auto titles = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("title")->chunk(0));
  std::string titlesExpected = std::string("[\n\
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
  GALOIS_ASSERT(titles->ToString() == titlesExpected);

  auto released = safe_cast<arrow::Int64Array>(
      graph.nodeProperties->GetColumnByName("released")->chunk(0));
  std::string releasedExpected = std::string("[\n\
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
  GALOIS_ASSERT(released->ToString() == releasedExpected);

  auto borns = safe_cast<arrow::StringArray>(
      graph.nodeProperties->GetColumnByName("born")->chunk(0));
  std::string bornsExpected = std::string("[\n\
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
  GALOIS_ASSERT(borns->ToString() == bornsExpected);

  // test node labels
  auto movies = safe_cast<arrow::BooleanArray>(
      graph.nodeLabels->GetColumnByName("Movie")->chunk(0));
  std::string moviesExpected = std::string("[\n\
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
  GALOIS_ASSERT(movies->ToString() == moviesExpected);

  auto persons = safe_cast<arrow::BooleanArray>(
      graph.nodeLabels->GetColumnByName("Person")->chunk(0));
  std::string personsExpected = std::string("[\n\
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
  GALOIS_ASSERT(persons->ToString() == personsExpected);

  auto others = safe_cast<arrow::BooleanArray>(
      graph.nodeLabels->GetColumnByName("Other")->chunk(0));
  std::string othersExpected = std::string("[\n\
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
  GALOIS_ASSERT(others->ToString() == othersExpected);

  auto randoms = safe_cast<arrow::BooleanArray>(
      graph.nodeLabels->GetColumnByName("Random")->chunk(0));
  std::string randomsExpected = std::string("[\n\
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
  GALOIS_ASSERT(randoms->ToString() == randomsExpected);

  // test edge properties
  auto roles = safe_cast<arrow::ListArray>(
      graph.edgeProperties->GetColumnByName("roles")->chunk(0));
  std::string rolesExpected = std::string("[\n\
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
  GALOIS_ASSERT(roles->ToString() == rolesExpected);

  auto numbers = safe_cast<arrow::ListArray>(
      graph.edgeProperties->GetColumnByName("numbers")->chunk(0));
  std::string numbersExpected = std::string("[\n\
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
  GALOIS_ASSERT(numbers->ToString() == numbersExpected);

  auto bools = safe_cast<arrow::ListArray>(
      graph.edgeProperties->GetColumnByName("bools")->chunk(0));
  std::string boolsExpected = std::string("[\n\
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
  GALOIS_ASSERT(bools->ToString() == boolsExpected);

  auto texts = safe_cast<arrow::StringArray>(
      graph.edgeProperties->GetColumnByName("text")->chunk(0));
  std::string textsExpected = std::string("[\n\
  null,\n\
  null,\n\
  null,\n\
  \"stuff\",\n\
  null,\n\
  null,\n\
  null,\n\
  null\n\
]");
  GALOIS_ASSERT(texts->ToString() == textsExpected);

  // test edge types
  auto actors = safe_cast<arrow::BooleanArray>(
      graph.edgeTypes->GetColumnByName("ACTED_IN")->chunk(0));
  std::string actorsExpected = std::string("[\n\
  true,\n\
  true,\n\
  true,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false\n\
]");
  GALOIS_ASSERT(actors->ToString() == actorsExpected);

  auto directors = safe_cast<arrow::BooleanArray>(
      graph.edgeTypes->GetColumnByName("DIRECTED")->chunk(0));
  std::string directorsExpected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  true,\n\
  false\n\
]");
  GALOIS_ASSERT(directors->ToString() == directorsExpected);

  auto producers = safe_cast<arrow::BooleanArray>(
      graph.edgeTypes->GetColumnByName("PRODUCED")->chunk(0));
  std::string producersExpected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  false,\n\
  true\n\
]");
  GALOIS_ASSERT(producers->ToString() == producersExpected);

  auto partners = safe_cast<arrow::BooleanArray>(
      graph.edgeTypes->GetColumnByName("IN_SAME_MOVIE")->chunk(0));
  std::string partnersExpected = std::string("[\n\
  false,\n\
  false,\n\
  false,\n\
  true,\n\
  false,\n\
  false,\n\
  false,\n\
  false\n\
]");
  GALOIS_ASSERT(partners->ToString() == partnersExpected);

  // test topology
  auto indices                = graph.topology->out_indices;
  std::string indicesExpected = std::string("[\n\
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
  GALOIS_ASSERT(indices->ToString() == indicesExpected);

  auto dests                = graph.topology->out_dests;
  std::string destsExpected = std::string("[\n\
  0,\n\
  0,\n\
  0,\n\
  7,\n\
  0,\n\
  0,\n\
  0,\n\
  0\n\
]");
  GALOIS_ASSERT(dests->ToString() == destsExpected);
}

} // namespace

int main(int argc, char** argv) {
  llvm::cl::ParseCommandLineOptions(argc, argv);

  galois::GraphComponents graph{nullptr, nullptr, nullptr, nullptr, nullptr};
  switch (fileType) {
  case galois::SourceType::GRAPHML:
    graph = galois::convertGraphML(inputFilename);
    break;
  case galois::SourceType::JSON:
    graph = galois::convertNeo4jJSON(inputFilename);
    break;
  case galois::SourceType::CSV:
    graph = galois::convertNeo4jCSV(inputFilename);
    break;
  }
  switch (testType) {
  case ConvertTest::MOVIES: {
    verifyMovieSet(graph);
    break;
  }
  case ConvertTest::TYPES: {
    verifyTypesSet(graph);
    break;
  }
  }
}

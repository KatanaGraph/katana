#include "graph-properties-convert.h"

#include <llvm/Support/CommandLine.h>

#include "galois/Galois.h"

namespace cll = llvm::cl;

static cll::opt<std::string> inputFilename(cll::Positional,
                                           cll::desc("<input file/directory>"),
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
  auto names = std::static_pointer_cast<arrow::StringArray>(
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

  auto taglines = std::static_pointer_cast<arrow::StringArray>(
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

  auto titles = std::static_pointer_cast<arrow::StringArray>(
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

  auto released = std::static_pointer_cast<arrow::StringArray>(
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

  auto borns = std::static_pointer_cast<arrow::StringArray>(
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
  auto movies = std::static_pointer_cast<arrow::BooleanArray>(
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

  auto persons = std::static_pointer_cast<arrow::BooleanArray>(
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

  auto others = std::static_pointer_cast<arrow::BooleanArray>(
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

  auto randoms = std::static_pointer_cast<arrow::BooleanArray>(
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
  auto roles = std::static_pointer_cast<arrow::StringArray>(
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

  auto texts = std::static_pointer_cast<arrow::StringArray>(
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
  auto actors = std::static_pointer_cast<arrow::BooleanArray>(
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

  auto directors = std::static_pointer_cast<arrow::BooleanArray>(
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

  auto producers = std::static_pointer_cast<arrow::BooleanArray>(
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

  auto partners = std::static_pointer_cast<arrow::BooleanArray>(
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

int main(int argc, char** argv) {
  llvm::cl::ParseCommandLineOptions(argc, argv);

  galois::GraphComponents graph{nullptr, nullptr, nullptr, nullptr, nullptr};
  switch (type) {
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
  verifyMovieSet(graph);
}

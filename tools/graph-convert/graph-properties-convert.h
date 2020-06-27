#ifndef GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_H_

#include <string>

#include <arrow/api.h>
#include "galois/graphs/PropertyFileGraph.h"

namespace galois {

enum SourceType { GRAPHML, JSON, CSV };
enum SourceDatabase { NONE, NEO4J, MONGODB };
enum ImportDataType { STRING, INT64, INT32, DOUBLE, FLOAT, BOOLEAN };

struct GraphComponents {
  std::shared_ptr<arrow::Table> nodeProperties;
  std::shared_ptr<arrow::Table> nodeLabels;
  std::shared_ptr<arrow::Table> edgeProperties;
  std::shared_ptr<arrow::Table> edgeTypes;
  std::shared_ptr<galois::graphs::GraphTopology> topology;

  GraphComponents(std::shared_ptr<arrow::Table> nodeProperties_,
                  std::shared_ptr<arrow::Table> nodeLabels_,
                  std::shared_ptr<arrow::Table> edgeProperties_,
                  std::shared_ptr<arrow::Table> edgeTypes_,
                  std::shared_ptr<galois::graphs::GraphTopology> topology_)
      : nodeProperties(nodeProperties_), nodeLabels(nodeLabels_),
        edgeProperties(edgeProperties_), edgeTypes(edgeTypes_),
        topology(topology_) {}
};

GraphComponents convertGraphML(const std::string& inputFilename);
GraphComponents convertNeo4jJSON(const std::string& inputFilename);
GraphComponents convertNeo4jCSV(const std::string& inputFilename);
void convertToPropertyGraphAndWrite(const GraphComponents& graphComps,
                                    const std::string& dir);

} // end of namespace galois

#endif

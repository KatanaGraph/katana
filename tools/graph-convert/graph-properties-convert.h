#ifndef GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_H_

#include <string>

#include <arrow/api.h>
#include "galois/graphs/PropertyFileGraph.h"

namespace galois {

enum SourceType { kGraphml };
enum SourceDatabase { kNone, kNeo4j, kMongodb };
enum ImportDataType { kString, kInt64, kInt32, kDouble, kFloat, kBoolean };

struct GraphComponents {
  std::shared_ptr<arrow::Table> node_properties;
  std::shared_ptr<arrow::Table> node_labels;
  std::shared_ptr<arrow::Table> edge_properties;
  std::shared_ptr<arrow::Table> edge_types;
  std::shared_ptr<galois::graphs::GraphTopology> topology;

  GraphComponents(std::shared_ptr<arrow::Table> node_properties_,
                  std::shared_ptr<arrow::Table> node_labels_,
                  std::shared_ptr<arrow::Table> edge_properties_,
                  std::shared_ptr<arrow::Table> edge_types_,
                  std::shared_ptr<galois::graphs::GraphTopology> topology_)
      : node_properties(node_properties_), node_labels(node_labels_),
        edge_properties(edge_properties_), edge_types(edge_types_),
        topology(topology_) {}
};

GraphComponents ConvertGraphml(const std::string& input_filename,
                               const size_t chunkSize);
void ConvertToPropertyGraphAndWrite(const GraphComponents& graph_comps,
                                    const std::string& dir);

} // end of namespace galois

#endif

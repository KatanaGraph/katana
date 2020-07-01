#ifndef GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_H_

#include <string>
#include <utility>

#include <arrow/api.h>
#include "galois/graphs/PropertyFileGraph.h"

namespace galois {

enum SourceType { kGraphml, kKatana };
enum SourceDatabase { kNone, kNeo4j, kMongodb };
enum ImportDataType {
  kString,
  kInt64,
  kInt32,
  kDouble,
  kFloat,
  kBoolean,
  kTimestampMilli,
  kStruct,
  kUnsupported
};

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
      : node_properties(std::move(node_properties_)),
        node_labels(std::move(node_labels_)),
        edge_properties(std::move(edge_properties_)),
        edge_types(std::move(edge_types_)), topology(std::move(topology_)) {}

  GraphComponents()
      : GraphComponents(nullptr, nullptr, nullptr, nullptr, nullptr) {}
};

GraphComponents ConvertGraphML(const std::string& input_filename,
                               size_t chunk_size);
galois::graphs::PropertyFileGraph
ConvertKatana(const std::string& input_filename);

void WritePropertyGraph(const GraphComponents& graph_comps,
                        const std::string& dir);
void WritePropertyGraph(graphs::PropertyFileGraph prop_graph,
                        const std::string& dir);
GraphComponents ConvertMongoDB(const std::string& dbName,
                               const size_t chunk_size);

} // end of namespace galois

#endif

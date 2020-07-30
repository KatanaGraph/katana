#ifndef GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_H_

#include <functional>
#include <string>
#include <vector>
#include <utility>

#include <arrow/api.h>
#include <libxml/xmlreader.h>
#include <libxml/xmlwriter.h>

#include "galois/graphs/PropertyFileGraph.h"

namespace galois {

using ArrayBuilders   = std::vector<std::shared_ptr<arrow::ArrayBuilder>>;
using BooleanBuilders = std::vector<std::shared_ptr<arrow::BooleanBuilder>>;
using ArrowArrays     = std::vector<std::shared_ptr<arrow::Array>>;
using ArrowFields     = std::vector<std::shared_ptr<arrow::Field>>;
using NullMaps =
    std::pair<std::unordered_map<int, std::shared_ptr<arrow::Array>>,
              std::unordered_map<int, std::shared_ptr<arrow::Array>>>;

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

struct PropertyKey {
  std::string id;
  bool for_node;
  bool for_edge;
  std::string name;
  ImportDataType type;
  bool is_list;

  PropertyKey(const std::string& id_, bool for_node_, bool for_edge_,
              const std::string& name_, ImportDataType type_, bool is_list_)
      : id(id_), for_node(for_node_), for_edge(for_edge_), name(name_),
        type(type_), is_list(is_list_) {}
  PropertyKey(const std::string& id, ImportDataType type, bool is_list)
      : PropertyKey(id, false, false, id, type, is_list) {}
};

struct LabelRule {
  std::string id;
  bool for_node;
  bool for_edge;
  std::string label;

  LabelRule(const std::string& id_, bool for_node_, bool for_edge_,
            const std::string& label_)
      : id(id_), for_node(for_node_), for_edge(for_edge_), label(label_) {}
  LabelRule(const std::string& id, const std::string& label)
      : LabelRule(id, false, false, label) {}
  LabelRule(const std::string& label) : LabelRule(label, false, false, label) {}
};

struct PropertiesState {
  std::unordered_map<std::string, size_t> keys;
  ArrowFields schema;
  ArrayBuilders builders;
  std::vector<ArrowArrays> chunks;
};

struct LabelsState {
  std::unordered_map<std::string, size_t> keys;
  ArrowFields schema;
  BooleanBuilders builders;
  std::vector<ArrowArrays> chunks;
  std::unordered_map<std::string, std::string> reverse_schema;
};

struct TopologyState {
  // maps node IDs to node indexes
  std::unordered_map<std::string, size_t> node_indexes;
  // node's start of edge lists
  std::vector<uint64_t> out_indices;
  // edge list of destinations
  std::vector<uint32_t> out_dests;
  // list of sources of edges
  std::vector<uint32_t> sources;
  // list of destinations of edges
  std::vector<uint32_t> destinations;

  // for schema mapping
  std::unordered_set<std::string> edge_ids;
  // for data ingestion that does not guarantee nodes are imported first
  std::vector<std::string> sources_intermediate;
  std::vector<std::string> destinations_intermediate;
};

struct GraphState {
  PropertiesState node_properties;
  PropertiesState edge_properties;
  LabelsState node_labels;
  LabelsState edge_types;
  TopologyState topology_builder;
  size_t nodes;
  size_t edges;
};

struct WriterProperties {
  NullMaps null_arrays;
  std::shared_ptr<arrow::Array> false_array;
  const size_t chunk_size;
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

galois::graphs::PropertyFileGraph
ConvertKatana(const std::string& input_filename);

xmlTextWriterPtr CreateGraphmlFile(const std::string& outfile);
void WriteGraphmlRule(xmlTextWriterPtr writer, const LabelRule& rule);
void WriteGraphmlKey(xmlTextWriterPtr writer, const PropertyKey& key);
void FinishGraphmlFile(xmlTextWriterPtr writer);
void ExportSchemaMapping(const std::string& outfile,
                         const std::vector<LabelRule>& rules,
                         const std::vector<PropertyKey>& keys);

ImportDataType ExtractTypeGraphML(xmlChar* value);
PropertyKey ProcessKey(xmlTextReaderPtr reader);
LabelRule ProcessRule(xmlTextReaderPtr reader);
std::pair<std::vector<std::string>, std::vector<std::string>>
ProcessSchemaMapping(GraphState* builder, const std::string& mapping,
                     const std::vector<std::string>& coll_names);

std::string TypeName(ImportDataType type);
ImportDataType ParseType(const std::string& in);

WriterProperties GetWriterProperties(size_t chunk_size);

size_t AddLabelBuilder(LabelsState* labels, LabelRule rule);
size_t AddBuilder(PropertiesState* properties, PropertyKey key);

void AddValue(std::shared_ptr<arrow::ArrayBuilder> builder, ArrowArrays* chunks,
              WriterProperties* properties, size_t total,
              std::function<void(void)> AppendValue);
void AddLabel(std::shared_ptr<arrow::BooleanBuilder> builder,
              ArrowArrays* chunks, WriterProperties* properties, size_t total);

GraphComponents BuildGraphComponents(GraphState builder,
                                     WriterProperties properties);

void WritePropertyGraph(const GraphComponents& graph_comps,
                        const std::string& dir);
void WritePropertyGraph(graphs::PropertyFileGraph prop_graph,
                        const std::string& dir);

} // end of namespace galois

#endif

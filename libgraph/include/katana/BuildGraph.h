#ifndef KATANA_LIBGRAPH_KATANA_BUILDGRAPH_H_
#define KATANA_LIBGRAPH_KATANA_BUILDGRAPH_H_

/// Construct a PropertyGraph in memory.
///
/// \file

#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/api.h>
#include <arrow/stl.h>
#include <arrow/type.h>

#include "katana/PropertyGraph.h"
#include "katana/URI.h"

namespace katana {

using ArrayBuilders = std::vector<std::shared_ptr<arrow::ArrayBuilder>>;
using BooleanBuilders = std::vector<std::shared_ptr<arrow::BooleanBuilder>>;
using ArrowArrays = std::vector<std::shared_ptr<arrow::Array>>;
using ArrowFields = std::vector<std::shared_ptr<arrow::Field>>;
using NullMaps = std::pair<
    std::unordered_map<int, std::shared_ptr<arrow::Array>>,
    std::unordered_map<int, std::shared_ptr<arrow::Array>>>;

enum SourceType { kGraphml, kKatana };
enum SourceDatabase { kNone, kNeo4j, kMongodb, kMysql };
enum ImportDataType {
  kString,
  kInt64,
  kInt32,
  kUInt32,
  kDouble,
  kFloat,
  kBoolean,
  kTimestampMilli,
  kStruct,
  kUnsupported
};

struct ImportData {
  void ValueFromArrowScalar(std::shared_ptr<arrow::Scalar> scalar);
  ImportDataType type;
  bool is_list;
  std::variant<
      uint8_t, std::string, int64_t, int32_t, uint32_t, double, float, bool,
      std::vector<std::string>, std::vector<int64_t>, std::vector<int32_t>,
      std::vector<double>, std::vector<float>, std::vector<bool>>
      value;

  ImportData(ImportDataType type_, bool is_list_)
      : type(type_), is_list(is_list_) {}
  ImportData(std::shared_ptr<arrow::Scalar> scalar) : is_list(false) {
    ValueFromArrowScalar(scalar);
  }
};

struct PropertyKey {
  std::string id;
  bool for_node;
  bool for_edge;
  std::string name;
  ImportDataType type;
  bool is_list;

  PropertyKey(
      const std::string& id_, bool for_node_, bool for_edge_,
      const std::string& name_, ImportDataType type_, bool is_list_)
      : id(id_),
        for_node(for_node_),
        for_edge(for_edge_),
        name(name_),
        type(type_),
        is_list(is_list_) {}
  PropertyKey(const std::string& id, ImportDataType type, bool is_list)
      : PropertyKey(id, false, false, id, type, is_list) {}
};

struct LabelRule {
  std::string id;
  bool for_node;
  bool for_edge;
  std::string label;

  LabelRule(
      const std::string& id_, bool for_node_, bool for_edge_,
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
  std::unordered_map<size_t, std::string> sources_intermediate;
  std::unordered_map<size_t, std::string> destinations_intermediate;
};

struct WriterProperties {
  NullMaps null_arrays;
  std::shared_ptr<arrow::Array> false_array;
  const size_t chunk_size;
};

struct GraphComponent {
  std::shared_ptr<arrow::Table> properties;
  std::shared_ptr<arrow::Table> labels;

  GraphComponent(
      std::shared_ptr<arrow::Table> properties_,
      std::shared_ptr<arrow::Table> labels_)
      : properties(properties_), labels(labels_) {}
  GraphComponent() : properties(nullptr), labels(nullptr) {}
};

struct KATANA_EXPORT GraphComponents {
  GraphComponent nodes;
  GraphComponent edges;
  GraphTopology topology;

  GraphComponents(
      GraphComponent nodes_, GraphComponent edges_, GraphTopology&& topology_)
      : nodes(std::move(nodes_)),
        edges(std::move(edges_)),
        topology(std::move(topology_)) {}

  GraphComponents() = default;

  void Dump() const {
    std::cout << nodes.properties->ToString() << "\n";
    std::cout << nodes.labels->ToString() << "\n";
    std::cout << edges.properties->ToString() << "\n";
    std::cout << edges.labels->ToString() << "\n";

    auto indices_array =
        katana::ProjectAsArrowArray(topology.AdjData(), topology.NumNodes());
    auto dests_array =
        katana::ProjectAsArrowArray(topology.DestData(), topology.NumEdges());

    std::cout << indices_array->ToString() << "\n";
    std::cout << dests_array->ToString() << "\n";
  }
};

class KATANA_EXPORT PropertyGraphBuilder {
  WriterProperties properties_;
  PropertiesState node_properties_;
  PropertiesState edge_properties_;
  LabelsState node_labels_;
  LabelsState edge_types_;
  TopologyState topology_builder_;
  size_t nodes_;
  size_t edges_;
  bool building_node_;
  bool building_edge_;

public:
  PropertyGraphBuilder(size_t chunk_size);

  bool StartNode();
  bool StartNode(const std::string& id);
  void AddNodeID(const std::string& id);
  void AddOutgoingEdge(const std::string& target, const std::string& label);
  void AddOutgoingEdge(uint32_t target, const std::string& label);
  bool FinishNode();

  bool AddNode(const std::string& id);

  bool StartEdge();
  bool StartEdge(const std::string& source, const std::string& target);
  void AddEdgeID(const std::string& id);
  void AddEdgeSource(const std::string& source);
  void AddEdgeTarget(const std::string& target);
  bool FinishEdge();

  bool AddEdge(const std::string& source, const std::string& target);
  bool AddEdge(
      uint32_t source, const std::string& target, const std::string& label);
  bool AddEdge(uint32_t source, uint32_t target, const std::string& label);

  size_t AddLabelBuilder(const LabelRule& rule);
  size_t AddBuilder(const PropertyKey& key);

  void AddValue(
      const std::string& id, std::function<PropertyKey()> ProcessElement,
      std::function<ImportData(ImportDataType, bool)> ResolveValue);
  void AddLabel(const std::string& name);

  Result<GraphComponents> Finish(bool verbose = true);

  size_t GetNodeIndex();
  size_t GetNodes();
  size_t GetEdges();

private:
  void ResolveIntermediateIDs();
  GraphComponent BuildFinalEdges(bool verbose);
};

KATANA_EXPORT Result<std::unique_ptr<katana::PropertyGraph>>
ConvertToPropertyGraph(
    GraphComponents&& graph_comps, katana::TxnContext* txn_ctx);

KATANA_EXPORT Result<void> WritePropertyGraph(
    GraphComponents&& graph_comps, const katana::URI& dir,
    katana::TxnContext* txn_ctx);

// TODO(amber): Take PropertyGraph by const ref
KATANA_EXPORT Result<void> WritePropertyGraph(
    PropertyGraph& prop_graph, const katana::URI& dir,
    katana::TxnContext* txn_ctx);

/// Convert Arrow chunked array to/from a vector of ImportData
KATANA_EXPORT std::vector<ImportData> ArrowToImport(
    const std::shared_ptr<arrow::ChunkedArray>& arr);
KATANA_EXPORT Result<std::shared_ptr<arrow::ChunkedArray>> ImportToArrow(
    arrow::Type::type arrow_type, const std::vector<ImportData>& import_src);

}  // namespace katana

#endif

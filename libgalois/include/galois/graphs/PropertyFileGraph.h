#ifndef GALOIS_LIBGALOIS_GALOIS_GRAPHS_PROPERTYFILEGRAPH_H_
#define GALOIS_LIBGALOIS_GALOIS_GRAPHS_PROPERTYFILEGRAPH_H_

#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <arrow/type_traits.h>

#include "galois/ErrorCode.h"
#include "galois/LargeArray.h"
#include "galois/config.h"
#include "tsuba/RDG.h"

namespace galois::graphs {

/// A graph topology represents the adjacency information for a graph in CSR
/// format.
struct GraphTopology {
  std::shared_ptr<arrow::UInt64Array> out_indices;
  std::shared_ptr<arrow::UInt32Array> out_dests;

  uint64_t num_nodes() const { return out_indices ? out_indices->length() : 0; }

  uint64_t num_edges() const { return out_dests ? out_dests->length() : 0; }

  bool Equals(const GraphTopology& other) const {
    return out_indices->Equals(*other.out_indices) &&
           out_dests->Equals(*other.out_dests);
  }

  std::pair<uint64_t, uint64_t> edge_range(uint32_t node_id) const {
    auto edge_start = node_id > 0 ? out_indices->Value(node_id - 1) : 0;
    auto edge_end = out_indices->Value(node_id);
    return std::make_pair(edge_start, edge_end);
  }
};

/// A property graph is a graph that has properties associated with its nodes
/// and edges. A property has a name and value. Its value may be a primitive
/// type, a list of values or a composition of properties.
///
/// A PropertyFileGraph is a representation of a property graph that is backed
/// by persistent storage, and it may be a subgraph of a larger, global property
/// graph. Another way to view a PropertyFileGraph is as a container for node
/// and edge properties that can be serialized.
///
/// The main way to load and store a property graph is via an RDG. An RDG
/// manages the serialization of the various partitions and properties that
/// comprise the physical representation of the logical property graph.
class GALOIS_EXPORT PropertyFileGraph {
  PropertyFileGraph(std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg);

  /// Validate performs a sanity check on the the graph after loading
  Result<void> Validate();

  Result<void> DoWrite(
      tsuba::RDGHandle handle, const std::string& command_line);
  Result<void> WriteGraph(
      const std::string& uri, const std::string& command_line);

  tsuba::RDG rdg_;
  std::unique_ptr<tsuba::RDGFile> file_;

  // The topology is either backed by rdg_ or shared with the
  // caller of SetTopology.
  GraphTopology topology_;

public:
  /// PropertyView provides a uniform interface when you don't need to
  /// distinguish operating on edge or node properties
  struct PropertyView {
    PropertyFileGraph* g;

    std::shared_ptr<arrow::Schema> (PropertyFileGraph::*schema_fn)() const;
    std::shared_ptr<arrow::ChunkedArray> (PropertyFileGraph::*property_fn)(
        int i) const;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> (
        PropertyFileGraph::*properties_fn)() const;
    Result<void> (PropertyFileGraph::*add_properties_fn)(
        const std::shared_ptr<arrow::Table>& table);
    Result<void> (PropertyFileGraph::*remove_property_fn)(int i);

    std::shared_ptr<arrow::Schema> schema() const { return (g->*schema_fn)(); }

    std::shared_ptr<arrow::ChunkedArray> Property(int i) const {
      return (g->*property_fn)(i);
    }

    std::vector<std::shared_ptr<arrow::ChunkedArray>> Properties() const {
      return (g->*properties_fn)();
    }

    Result<void> AddProperties(
        const std::shared_ptr<arrow::Table>& table) const {
      return (g->*add_properties_fn)(table);
    }

    Result<void> RemoveProperty(int i) const {
      return (g->*remove_property_fn)(i);
    }
  };

  PropertyFileGraph();

  /// Make a property graph from a constructed RDG. Take ownership of the RDG
  /// and its underlying resources.
  static Result<std::unique_ptr<PropertyFileGraph>> Make(
      std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg);

  /// Make a property graph from an RDG name.
  static Result<std::unique_ptr<PropertyFileGraph>> Make(
      const std::string& rdg_name);

  /// Make a property graph from an RDG but only load the named node and edge
  /// properties.
  ///
  /// The order of properties in the resulting graph will match the order of
  /// given in the property arguments.
  ///
  /// \returns invalid_argument if any property is not found or if there
  /// are multiple properties with the same name
  static Result<std::unique_ptr<PropertyFileGraph>> Make(
      const std::string& rdg_name,
      const std::vector<std::string>& node_properties,
      const std::vector<std::string>& edge_properties);

  const tsuba::PartitionMetadata& partition_metadata() const {
    return rdg_.part_metadata();
  }
  void set_partition_metadata(const tsuba::PartitionMetadata& meta) {
    rdg_.set_part_metadata(meta);
  }

  const std::shared_ptr<arrow::ChunkedArray>& local_to_global_vector() {
    return rdg_.local_to_global_vector();
  }
  void set_local_to_global_vector(std::shared_ptr<arrow::ChunkedArray>&& a) {
    rdg_.set_local_to_global_vector(std::move(a));
  }

  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& master_nodes() {
    return rdg_.master_nodes();
  }
  void set_master_nodes(std::vector<std::shared_ptr<arrow::ChunkedArray>>&& a) {
    rdg_.set_master_nodes(std::move(a));
  }

  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& mirror_nodes() {
    return rdg_.mirror_nodes();
  }
  void set_mirror_nodes(std::vector<std::shared_ptr<arrow::ChunkedArray>>&& a) {
    rdg_.set_mirror_nodes(std::move(a));
  }

  /// Write the property graph to the given RDG name.
  ///
  /// \returns io_error if, for instance, a file already exists
  Result<void> Write(
      const std::string& rdg_name, const std::string& command_line);

  /// Write updates to the property graph
  ///
  /// Like \ref Write(const std::string&, const std::string&) but update
  /// the original read location of the graph
  Result<void> Commit(const std::string& command_line);
  /// Tell the RDG where it's data is coming from
  Result<void> InformPath(const std::string& input_path) {
    if (!rdg_.rdg_dir().empty()) {
      GALOIS_LOG_DEBUG("rdg dir from {} to {}", rdg_.rdg_dir(), input_path);
    }
    auto uri_res = galois::Uri::Make(input_path);
    if (!uri_res) {
      return uri_res.error();
    }

    rdg_.set_rdg_dir(uri_res.value());
    return ResultSuccess();
  }

  /// Determine if two PropertyFileGraphss are Equal
  bool Equals(const PropertyFileGraph* other) const {
    return topology().Equals(other->topology()) &&
           rdg_.node_table()->Equals(*other->node_table()) &&
           rdg_.edge_table()->Equals(*other->edge_table());
  }

  std::shared_ptr<arrow::Schema> node_schema() const {
    return rdg_.node_table()->schema();
  }

  std::shared_ptr<arrow::Schema> edge_schema() const {
    return rdg_.edge_table()->schema();
  }

  std::shared_ptr<arrow::ChunkedArray> NodeProperty(int i) const {
    return rdg_.node_table()->column(i);
  }

  std::shared_ptr<arrow::ChunkedArray> EdgeProperty(int i) const {
    return rdg_.edge_table()->column(i);
  }

  /**
   * Get a node property by name.
   *
   * @param name The name of the property to get.
   * @return The property data or NULL if the property is not found.
   */
  std::shared_ptr<arrow::ChunkedArray> NodeProperty(
      const std::string& name) const {
    return rdg_.node_table()->GetColumnByName(name);
  }

  std::shared_ptr<arrow::ChunkedArray> EdgeProperty(
      const std::string& name) const {
    return rdg_.edge_table()->GetColumnByName(name);
  }

  /**
   * Get a node property by name specifying it's type.
   * @tparam T The type of the property.
   * @param name The name of the property.
   * @return The property array or an error if the property does not exist or has a different type.
   */
  template <typename T>
  Result<std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType>>
  NodePropertyTyped(const std::string& name) {
    auto chunked_array = NodeProperty(name);
    if (!chunked_array) {
      return ErrorCode::PropertyNotFound;
    }

    auto array =
        std::dynamic_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(
            chunked_array->chunk(0));
    if (!array) {
      return ErrorCode::TypeError;
    }
    return array;
  }

  /**
    * Get an edge property by name specifying it's type.
    * @tparam T The type of the property.
    * @param name The name of the property.
    * @return The property array or an error if the property does not exist or has a different type.
    */
  template <typename T>
  Result<std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType>>
  EdgePropertyTyped(const std::string& name) {
    auto chunked_array = EdgeProperty(name);
    if (!chunked_array) {
      return ErrorCode::PropertyNotFound;
    }

    auto array =
        std::dynamic_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(
            chunked_array->chunk(0));
    if (!array) {
      return ErrorCode::TypeError;
    }
    return array;
  }

  void MarkAllPropertiesPersistent() {
    return rdg_.MarkAllPropertiesPersistent();
  }

  /// MarkNodePropertiesPersistent indicates which node properties will be
  /// serialized when this graph is written.
  ///
  /// Properties are "named" by position, so an empty string means don't persist that
  /// property.
  Result<void> MarkNodePropertiesPersistent(
      const std::vector<std::string>& persist_node_props) {
    return rdg_.MarkNodePropertiesPersistent(persist_node_props);
  }

  Result<void> MarkEdgePropertiesPersistent(
      const std::vector<std::string>& persist_edge_props) {
    return rdg_.MarkEdgePropertiesPersistent(persist_edge_props);
  }

  const GraphTopology& topology() const { return topology_; }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> NodeProperties() const {
    return rdg_.node_table()->columns();
  }
  std::vector<std::string> NodePropertyNames() const {
    return rdg_.node_table()->ColumnNames();
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> EdgeProperties() const {
    return rdg_.edge_table()->columns();
  }
  std::vector<std::string> EdgePropertyNames() const {
    return rdg_.edge_table()->ColumnNames();
  }

  Result<void> AddNodeProperties(const std::shared_ptr<arrow::Table>& table);
  Result<void> AddEdgeProperties(const std::shared_ptr<arrow::Table>& table);

  Result<void> RemoveNodeProperty(int i) { return rdg_.RemoveNodeProperty(i); }
  Result<void> RemoveNodeProperty(const std::string& prop_name) {
    auto col_names = NodePropertyNames();
    auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
    if (pos != col_names.cend()) {
      return rdg_.RemoveNodeProperty(std::distance(col_names.cbegin(), pos));
    }
    return galois::ErrorCode::PropertyNotFound;
  }
  Result<void> RemoveEdgeProperty(int i) { return rdg_.RemoveEdgeProperty(i); }
  Result<void> RemoveEdgeProperty(const std::string& prop_name) {
    auto col_names = EdgePropertyNames();
    auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
    if (pos != col_names.cend()) {
      return rdg_.RemoveNodeProperty(std::distance(col_names.cbegin(), pos));
    }
    return galois::ErrorCode::PropertyNotFound;
  }

  PropertyView node_property_view() {
    return PropertyView{
        .g = this,
        .schema_fn = &PropertyFileGraph::node_schema,
        .property_fn = &PropertyFileGraph::NodeProperty,
        .properties_fn = &PropertyFileGraph::NodeProperties,
        .add_properties_fn = &PropertyFileGraph::AddNodeProperties,
        .remove_property_fn = &PropertyFileGraph::RemoveNodeProperty,
    };
  }

  PropertyView edge_property_view() {
    return PropertyView{
        .g = this,
        .schema_fn = &PropertyFileGraph::edge_schema,
        .property_fn = &PropertyFileGraph::EdgeProperty,
        .properties_fn = &PropertyFileGraph::EdgeProperties,
        .add_properties_fn = &PropertyFileGraph::AddEdgeProperties,
        .remove_property_fn = &PropertyFileGraph::RemoveEdgeProperty,
    };
  }

  Result<void> SetTopology(const GraphTopology& topology);

  const std::shared_ptr<arrow::Table>& node_table() const {
    return rdg_.node_table();
  }
  const std::shared_ptr<arrow::Table>& edge_table() const {
    return rdg_.edge_table();
  }
};

/// SortAllEdgesByDest sorts edges for each node by destination
/// ids (ascending order).
///
/// This function modifies the PropertyFileGraph topology by doing
/// in-place sorting of the edgelists of each nodes in the
/// ascending order.
/// This also returns the permutation vector (mapping from old
/// indices to the new indices) which results due to the sorting.
GALOIS_EXPORT Result<std::vector<uint64_t>> SortAllEdgesByDest(
    PropertyFileGraph* pfg);

/// FindEdgeSortedByDest finds the "node_to_find" id in the
/// sorted edgelist of the "node" using binary search.
///
/// This returns the matched edge index if 'node_to_find' is present
/// in the edgelist of 'node' else edge end if 'node_to_find' is not found.
GALOIS_EXPORT uint64_t FindEdgeSortedByDest(
    const PropertyFileGraph& graph, uint32_t node, uint32_t node_to_find);

/// SortNodesByDegree relables node ids by sorting in the descending
/// order by node degree
///
/// This function modifies the PropertyFileGraph topology by in-place
/// relabeling and sorting the node ids by their degree in the
/// descending order.
GALOIS_EXPORT Result<void> SortNodesByDegree(PropertyFileGraph* pfg);

}  // namespace galois::graphs

#endif

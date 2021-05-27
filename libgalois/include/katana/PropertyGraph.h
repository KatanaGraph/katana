#ifndef KATANA_LIBGALOIS_KATANA_PROPERTYGRAPH_H_
#define KATANA_LIBGALOIS_KATANA_PROPERTYGRAPH_H_

#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <arrow/type_traits.h>

#include "katana/Details.h"
#include "katana/ErrorCode.h"
#include "katana/LargeArray.h"
#include "katana/config.h"
#include "tsuba/RDG.h"

namespace katana {

/// A graph topology represents the adjacency information for a graph in CSR
/// format.
struct KATANA_EXPORT GraphTopology {
  using Node = uint32_t;
  using Edge = uint64_t;
  using node_iterator = boost::counting_iterator<Node>;
  using edge_iterator = boost::counting_iterator<Edge>;
  using nodes_range = StandardRange<node_iterator>;
  using edges_range = StandardRange<edge_iterator>;
  using iterator = node_iterator;

  std::shared_ptr<arrow::UInt64Array> out_indices;
  std::shared_ptr<arrow::UInt32Array> out_dests;

  uint64_t num_nodes() const { return out_indices ? out_indices->length() : 0; }

  uint64_t num_edges() const { return out_dests ? out_dests->length() : 0; }

  bool Equals(const GraphTopology& other) const {
    return out_indices->Equals(*other.out_indices) &&
           out_dests->Equals(*other.out_dests);
  }

  // Edge accessors

  // TODO(amp): [[deprecated("use edges(node)")]]
  std::pair<Edge, Edge> edge_range(Node node_id) const {
    auto edge_start = node_id > 0 ? out_indices->Value(node_id - 1) : 0;
    auto edge_end = out_indices->Value(node_id);
    return std::make_pair(edge_start, edge_end);
  }

  /// Gets the edge range of some node.
  ///
  /// \param node an iterator pointing to the node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range edges(const node_iterator& node) const { return edges(*node); }
  // TODO(amp): [[deprecated("use edges(Node node)")]]

  /// Gets the edge range of some node.
  ///
  /// \param node node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range edges(Node node) const {
    auto [begin_edge, end_edge] = edge_range(node);
    return MakeStandardRange<edge_iterator>(begin_edge, end_edge);
  }
  Node edge_dest(Edge eid) const {
    KATANA_LOG_ASSERT(eid < static_cast<Edge>(out_dests->length()));
    return out_dests->Value(eid);
  }

  nodes_range nodes(Node begin, Node end) const {
    return MakeStandardRange<node_iterator>(begin, end);
  }

  // Standard container concepts

  node_iterator begin() const { return node_iterator(0); }

  node_iterator end() const { return node_iterator(num_nodes()); }

  size_t size() const { return num_nodes(); }

  bool empty() const { return num_nodes() == 0; }
};

/// A property graph is a graph that has properties associated with its nodes
/// and edges. A property has a name and value. Its value may be a primitive
/// type, a list of values or a composition of properties.
///
/// A PropertyGraph is a representation of a property graph that is backed
/// by persistent storage, and it may be a subgraph of a larger, global property
/// graph. Another way to view a PropertyGraph is as a container for node
/// and edge properties that can be serialized.
///
/// The main way to load and store a property graph is via an RDG. An RDG
/// manages the serialization of the various partitions and properties that
/// comprise the physical representation of the logical property graph.
class KATANA_EXPORT PropertyGraph {
  PropertyGraph(std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg);

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

  // Keep partition_metadata, master_nodes, mirror_nodes out of the public interface,
  // while allowing Distribution to read/write it for RDG
  friend class Distribution;
  const tsuba::PartitionMetadata& partition_metadata() const {
    return rdg_.part_metadata();
  }
  void set_partition_metadata(const tsuba::PartitionMetadata& meta) {
    rdg_.set_part_metadata(meta);
  }

  /// Per-host vector of master nodes
  ///
  /// master_nodes()[this_host].empty() is true
  /// master_nodes()[host_i][x] contains LocalNodeID of masters
  //    for which host_i has a mirror
  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& master_nodes()
      const {
    return rdg_.master_nodes();
  }
  void set_master_nodes(std::vector<std::shared_ptr<arrow::ChunkedArray>>&& a) {
    rdg_.set_master_nodes(std::move(a));
  }

  /// Per-host vector of mirror nodes
  ///
  /// mirror_nodes()[this_host].empty() is true
  /// mirror_nodes()[host_i][x] contains LocalNodeID of mirrors
  ///   that have a master on host_i
  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& mirror_nodes()
      const {
    return rdg_.mirror_nodes();
  }
  void set_mirror_nodes(std::vector<std::shared_ptr<arrow::ChunkedArray>>&& a) {
    rdg_.set_mirror_nodes(std::move(a));
  }

public:
  /// PropertyView provides a uniform interface when you don't need to
  /// distinguish operating on edge or node properties
  struct PropertyView {
    PropertyGraph* g;

    std::shared_ptr<arrow::Schema> (PropertyGraph::*schema_fn)() const;
    std::shared_ptr<arrow::ChunkedArray> (PropertyGraph::*property_fn)(
        int i) const;
    const std::shared_ptr<arrow::Table>& (
        PropertyGraph::*properties_fn)() const;
    Result<void> (PropertyGraph::*add_properties_fn)(
        const std::shared_ptr<arrow::Table>& props);
    Result<void> (PropertyGraph::*upsert_properties_fn)(
        const std::shared_ptr<arrow::Table>& props);
    Result<void> (PropertyGraph::*remove_property_int)(int i);
    Result<void> (PropertyGraph::*remove_property_str)(const std::string& str);

    std::shared_ptr<arrow::Schema> schema() const { return (g->*schema_fn)(); }

    std::shared_ptr<arrow::ChunkedArray> Property(int i) const {
      return (g->*property_fn)(i);
    }

    const std::shared_ptr<arrow::Table>& properties() const {
      return (g->*properties_fn)();
    }

    std::vector<std::string> property_names() const {
      return properties()->ColumnNames();
    }

    Result<void> AddProperties(
        const std::shared_ptr<arrow::Table>& props) const {
      return (g->*add_properties_fn)(props);
    }

    Result<void> UpsertProperties(
        const std::shared_ptr<arrow::Table>& props) const {
      return (g->*upsert_properties_fn)(props);
    }

    Result<void> RemoveProperty(int i) const {
      return (g->*remove_property_int)(i);
    }
    Result<void> RemoveProperty(const std::string& str) const {
      return (g->*remove_property_str)(str);
    }
  };

  PropertyGraph();

  /// Make a property graph from a constructed RDG. Take ownership of the RDG
  /// and its underlying resources.
  static Result<std::unique_ptr<PropertyGraph>> Make(
      std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg);

  /// Make a property graph from an RDG name.
  static Result<std::unique_ptr<PropertyGraph>> Make(
      const std::string& rdg_name,
      const tsuba::RDGLoadOptions& opts = tsuba::RDGLoadOptions());

  /// \return A copy of this with the same set of properties. The copy shares no
  ///       state with this.
  Result<std::unique_ptr<PropertyGraph>> Copy() const;

  /// \param node_properties The node properties to copy.
  /// \param edge_properties The edge properties to copy.
  /// \return A copy of this with a subset of the properties. The copy shares no
  ///        state with this.
  Result<std::unique_ptr<PropertyGraph>> Copy(
      const std::vector<std::string>& node_properties,
      const std::vector<std::string>& edge_properties) const;

  const std::string& rdg_dir() const { return rdg_.rdg_dir().string(); }

  uint32_t partition_id() const { return rdg_.partition_id(); }

  // TODO(witchel): ChunkedArray is inherited from arrow::Table interface but this is
  // really a ChunkedArray of one chunk, change to arrow::Array.
  const std::shared_ptr<arrow::ChunkedArray>& host_to_owned_global_node_ids()
      const {
    return rdg_.host_to_owned_global_node_ids();
  }
  void set_host_to_owned_global_node_ids(
      std::shared_ptr<arrow::ChunkedArray>&& a) {
    rdg_.set_host_to_owned_global_node_ids(std::move(a));
  }

  // TODO(witchel): ChunkedArray is inherited from arrow::Table interface but this is
  // really a ChunkedArray of one chunk, change to arrow::Array.
  const std::shared_ptr<arrow::ChunkedArray>& host_to_owned_global_edge_ids()
      const {
    return rdg_.host_to_owned_global_edge_ids();
  }
  void set_host_to_owned_global_edge_ids(
      std::shared_ptr<arrow::ChunkedArray>&& a) {
    rdg_.set_host_to_owned_global_edge_ids(std::move(a));
  }

  // TODO(witchel): ChunkedArray is inherited from arrow::Table interface but this is
  // really a ChunkedArray of one chunk, change to arrow::Array.
  const std::shared_ptr<arrow::ChunkedArray>& local_to_user_id() const {
    return rdg_.local_to_user_id();
  }
  void set_local_to_user_id(std::shared_ptr<arrow::ChunkedArray>&& a) {
    rdg_.set_local_to_user_id(std::move(a));
  }

  // TODO(witchel): ChunkedArray is inherited from arrow::Table interface but this is
  // really a ChunkedArray of one chunk, change to arrow::Array.
  const std::shared_ptr<arrow::ChunkedArray>& local_to_global_id() const {
    return rdg_.local_to_global_id();
  }
  void set_local_to_global_id(std::shared_ptr<arrow::ChunkedArray>&& a) {
    rdg_.set_local_to_global_id(std::move(a));
  }

  /// Create a new storage location for a graph and write everything into it.
  ///
  /// \returns io_error if, for instance, a file already exists
  Result<void> Write(
      const std::string& rdg_name, const std::string& command_line);

  /// Commit updates modified state and re-uses graph components already in storage.
  ///
  /// Like \ref Write(const std::string&, const std::string&) but can only update
  /// parts of the original read location of the graph.
  Result<void> Commit(const std::string& command_line);
  /// Tell the RDG where it's data is coming from
  Result<void> InformPath(const std::string& input_path);

  /// Determine if two PropertyGraphs are Equal
  bool Equals(const PropertyGraph* other) const;
  /// Report the differences between two graphs
  std::string ReportDiff(const PropertyGraph* other) const;

  std::shared_ptr<arrow::Schema> node_schema() const {
    return node_properties()->schema();
  }

  std::shared_ptr<arrow::Schema> edge_schema() const {
    return edge_properties()->schema();
  }

  // Return type dictated by arrow
  int32_t GetNodePropertyNum() const {
    return node_properties()->num_columns();
  }
  int32_t GetEdgePropertyNum() const {
    return edge_properties()->num_columns();
  }

  // num_rows() == num_nodes() (all local nodes)
  std::shared_ptr<arrow::ChunkedArray> GetNodeProperty(int i) const {
    if (i >= rdg_.node_properties()->num_columns()) {
      return nullptr;
    }
    return node_properties()->column(i);
  }

  // num_rows() == num_edges() (all local edges)
  std::shared_ptr<arrow::ChunkedArray> GetEdgeProperty(int i) const {
    if (i >= edge_properties()->num_columns()) {
      return nullptr;
    }
    return edge_properties()->column(i);
  }

  /// Get a node property by name.
  ///
  /// \param name The name of the property to get.
  /// \return The property data or NULL if the property is not found.
  std::shared_ptr<arrow::ChunkedArray> GetNodeProperty(
      const std::string& name) const {
    return node_properties()->GetColumnByName(name);
  }
  std::vector<std::string> GetNodePropertyNames() const {
    return node_properties()->ColumnNames();
  }

  std::shared_ptr<arrow::ChunkedArray> GetEdgeProperty(
      const std::string& name) const {
    return edge_properties()->GetColumnByName(name);
  }
  std::vector<std::string> GetEdgePropertyNames() const {
    return edge_properties()->ColumnNames();
  }

  /// Get a node property by name and cast it to a type.
  ///
  /// \tparam T The type of the property.
  /// \param name The name of the property.
  /// \return The property array or an error if the property does not exist or has a different type.
  template <typename T>
  Result<std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType>>
  GetNodePropertyTyped(const std::string& name) {
    auto chunked_array = GetNodeProperty(name);
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

  /// Get an edge property by name and cast it to a type.
  ///
  /// \tparam T The type of the property.
  /// \param name The name of the property.
  /// \return The property array or an error if the property does not exist or has a different type.
  template <typename T>
  Result<std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType>>
  GetEdgePropertyTyped(const std::string& name) {
    auto chunked_array = GetEdgeProperty(name);
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

  /// Add Node properties that do not exist in the current graph
  Result<void> AddNodeProperties(const std::shared_ptr<arrow::Table>& props);
  /// Add Edge properties that do not exist in the current graph
  Result<void> AddEdgeProperties(const std::shared_ptr<arrow::Table>& props);
  /// If property name exists, replace it, otherwise insert it
  Result<void> UpsertNodeProperties(const std::shared_ptr<arrow::Table>& props);
  /// If property name exists, replace it, otherwise insert it
  Result<void> UpsertEdgeProperties(const std::shared_ptr<arrow::Table>& props);

  Result<void> RemoveNodeProperty(int i);
  Result<void> RemoveNodeProperty(const std::string& prop_name);

  Result<void> RemoveEdgeProperty(int i);
  Result<void> RemoveEdgeProperty(const std::string& prop_name);

  PropertyView node_property_view() {
    return PropertyView{
        .g = this,
        .schema_fn = &PropertyGraph::node_schema,
        .property_fn = &PropertyGraph::GetNodeProperty,
        .properties_fn = &PropertyGraph::node_properties,
        .add_properties_fn = &PropertyGraph::AddNodeProperties,
        .upsert_properties_fn = &PropertyGraph::UpsertNodeProperties,
        .remove_property_int = &PropertyGraph::RemoveNodeProperty,
        .remove_property_str = &PropertyGraph::RemoveNodeProperty,
    };
  }

  PropertyView edge_property_view() {
    return PropertyView{
        .g = this,
        .schema_fn = &PropertyGraph::edge_schema,
        .property_fn = &PropertyGraph::GetEdgeProperty,
        .properties_fn = &PropertyGraph::edge_properties,
        .add_properties_fn = &PropertyGraph::AddEdgeProperties,
        .upsert_properties_fn = &PropertyGraph::UpsertEdgeProperties,
        .remove_property_int = &PropertyGraph::RemoveEdgeProperty,
        .remove_property_str = &PropertyGraph::RemoveEdgeProperty,
    };
  }

  Result<void> SetTopology(const GraphTopology& topology);

  /// Return the node property table for local nodes
  const std::shared_ptr<arrow::Table>& node_properties() const {
    return rdg_.node_properties();
  }
  /// Return the edge property table for local edges
  const std::shared_ptr<arrow::Table>& edge_properties() const {
    return rdg_.edge_properties();
  }

  // Pass through topology API

  using node_iterator = GraphTopology::node_iterator;
  using edge_iterator = GraphTopology::edge_iterator;
  using edges_range = GraphTopology::edges_range;
  using iterator = GraphTopology::iterator;
  using Node = GraphTopology::Node;
  using Edge = GraphTopology::Edge;

  // Standard container concepts

  node_iterator begin() const { return topology().begin(); }

  node_iterator end() const { return topology().end(); }

  /// Return the number of local nodes
  size_t size() const { return topology().size(); }

  bool empty() const { return topology().empty(); }

  /// Return the number of local nodes
  ///  num_nodes in repartitioner is of type LocalNodeID
  uint64_t num_nodes() const { return topology().num_nodes(); }
  /// Return the number of local edges
  uint64_t num_edges() const { return topology().num_edges(); }

  /// Gets the edge range of some node.
  ///
  /// \param node node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range edges(Node node) const { return topology().edges(node); }

  /**
   * Gets the destination for an edge.
   *
   * @param edge edge iterator to get the destination of
   * @returns node iterator to the edge destination
   */
  node_iterator GetEdgeDest(const edge_iterator& edge) const {
    auto node_id = topology().edge_dest(*edge);
    return node_iterator(node_id);
  }
};

/// SortAllEdgesByDest sorts edges for each node by destination
/// IDs (ascending order).
///
/// Returns the permutation vector (mapping from old
/// indices to the new indices) which results due to the sorting.
KATANA_EXPORT Result<std::shared_ptr<arrow::UInt64Array>> SortAllEdgesByDest(
    PropertyGraph* pg);

/// FindEdgeSortedByDest finds the "node_to_find" id in the
/// sorted edgelist of the "node" using binary search.
///
/// This returns the matched edge index if 'node_to_find' is present
/// in the edgelist of 'node' else edge end if 'node_to_find' is not found.
KATANA_EXPORT GraphTopology::Edge FindEdgeSortedByDest(
    const PropertyGraph* graph, GraphTopology::Node node,
    GraphTopology::Node node_to_find);

/// Relabel all nodes in the graph by sorting in the descending
/// order by node degree.
KATANA_EXPORT Result<void> SortNodesByDegree(PropertyGraph* pg);

/// Creates in-memory symmetric (or undirected) graph.
///
/// This function creates an symmetric or undirected version of the
/// PropertyGraph topology by adding reverse edges in-memory.
///
/// For each edge (a, b) in the graph, this function will
/// add an additional edge (b, a) except when a == b, in which
/// case, no additional edge is added.
/// The generated symmetric graph may have duplicate edges.
/// \param pg The original property graph
/// \return The new symmetric property graph by adding reverse edges
// TODO(gill): Add edge properties for the new reversed edges.
KATANA_EXPORT Result<std::unique_ptr<katana::PropertyGraph>>
CreateSymmetricGraph(PropertyGraph* pg);

/// Creates in-memory transpose graph.
///
/// This function creates transpose version of the
/// PropertyGraph topology by reversing the edges in-memory.
///
/// For each edge (a, b) in the graph, this function will
/// add edge (b, a) without retaining the original edge (a, b) unlike
/// CreateSymmetricGraph.
/// \param pg The original property graph
/// \return The new transposed property graph by reversing the edges
// TODO(gill): Add tranposed edge properties as well.
KATANA_EXPORT Result<std::unique_ptr<katana::PropertyGraph>>
CreateTransposeGraph(PropertyGraph* pg);

}  // namespace katana

#endif

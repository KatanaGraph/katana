#ifndef KATANA_LIBGALOIS_KATANA_PROPERTYGRAPH_H_
#define KATANA_LIBGALOIS_KATANA_PROPERTYGRAPH_H_

#include <bitset>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <arrow/type_traits.h>

#include "katana/ArrowInterchange.h"
#include "katana/Details.h"
#include "katana/ErrorCode.h"
#include "katana/NUMAArray.h"
#include "katana/config.h"
#include "tsuba/RDG.h"

namespace katana {

// TODO(amber): find a better place to put this
template <
    typename T,
    typename __Unused = std::enable_if_t<std::is_arithmetic<T>::value>>
auto
ProjectAsArrowArray(const T* buf, const size_t len) noexcept {
  using ArrowDataType = typename arrow::CTypeTraits<T>::ArrowType;
  using ArrowArrayType = arrow::NumericArray<ArrowDataType>;
  return std::make_shared<ArrowArrayType>(len, arrow::Buffer::Wrap(buf, len));
}

/// A graph topology represents the adjacency information for a graph in CSR
/// format.
class KATANA_EXPORT GraphTopology {
public:
  using Node = uint32_t;
  using Edge = uint64_t;
  using node_iterator = boost::counting_iterator<Node>;
  using edge_iterator = boost::counting_iterator<Edge>;
  using nodes_range = StandardRange<node_iterator>;
  using edges_range = StandardRange<edge_iterator>;
  using iterator = node_iterator;

  GraphTopology() = default;
  GraphTopology(GraphTopology&&) = default;
  GraphTopology& operator=(GraphTopology&&) = default;

  GraphTopology(const GraphTopology&) = delete;
  GraphTopology& operator=(const GraphTopology&) = delete;

  GraphTopology(
      const Edge* adj_indices, size_t numNodes, const Node* dests,
      size_t numEdges) noexcept;

  GraphTopology(NUMAArray<Edge>&& adj_indices, NUMAArray<Node>&& dests) noexcept
      : adj_indices_(std::move(adj_indices)), dests_(std::move(dests)) {}

  static GraphTopology Copy(const GraphTopology& that) noexcept;

  uint64_t num_nodes() const noexcept { return adj_indices_.size(); }

  uint64_t num_edges() const noexcept { return dests_.size(); }

  const Edge* adj_data() const noexcept { return adj_indices_.data(); }

  const Node* dest_data() const noexcept { return dests_.data(); }

  /**
   * Checks equality against another instance of GraphTopology.
   * WARNING: Expensive operation due to element-wise checks on large arrays
   * @param that: GraphTopology instance to compare against
   * @returns true if topology arrays are equal
   */
  bool Equals(const GraphTopology& that) const noexcept {
    if (this == &that) {
      return true;
    }
    if (num_nodes() != that.num_nodes()) {
      return false;
    }
    if (num_edges() != that.num_edges()) {
      return false;
    }

    return adj_indices_ == that.adj_indices_ && dests_ == that.dests_;
  }

  // Edge accessors

  edge_iterator edge_begin(Node node) const noexcept {
    return edge_iterator{node > 0 ? adj_indices_[node - 1] : 0};
  }

  edge_iterator edge_end(Node node) const noexcept {
    return edge_iterator{adj_indices_[node]};
  }

  /// Gets the edge range of some node.
  ///
  /// \param node an iterator pointing to the node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range edges(const node_iterator& node) const noexcept {
    return edges(*node);
  }
  // TODO(amp): [[deprecated("use edges(Node node)")]]

  /// Gets the edge range of some node.
  ///
  /// \param node node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range edges(Node node) const noexcept {
    return MakeStandardRange<edge_iterator>(edge_begin(node), edge_end(node));
  }

  Node edge_dest(Edge edge_id) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(edge_id < dests_.size());
    return dests_[edge_id];
  }

  Node edge_dest(const edge_iterator ei) const noexcept {
    return edge_dest(*ei);
  }

  nodes_range nodes(Node begin, Node end) const noexcept {
    return MakeStandardRange<node_iterator>(begin, end);
  }

  // Standard container concepts

  node_iterator begin() const noexcept { return node_iterator(0); }

  node_iterator end() const noexcept { return node_iterator(num_nodes()); }

  size_t size() const noexcept { return num_nodes(); }

  bool empty() const noexcept { return num_nodes() == 0; }

private:
  NUMAArray<Edge> adj_indices_;
  NUMAArray<Node> dests_;
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
public:
  /// TypeSetID uniquely identifies/contains a combination/set of types
  /// TypeSetID is represented using 8 bits
  /// TypeSetID for nodes is distinct from TypeSetID for edges
  using TypeSetID = uint8_t;
  static constexpr TypeSetID kUnknownType = TypeSetID{0};
  static constexpr TypeSetID kInvalidType =
      std::numeric_limits<TypeSetID>::max();
  /// A set of TypeSetIDs
  using SetOfTypeSetIDs =
      std::bitset<std::numeric_limits<TypeSetID>::max() + 1>;
  /// A set of type names
  using SetOfTypeNames = std::unordered_set<std::string>;
  /// A map from TypeSetID to the set of the type names it contains
  using TypeSetIDToSetOfTypeNamesMap = std::vector<SetOfTypeNames>;
  /// A map from the type name to the set of the TypeSetIDs that contain it
  using TypeNameToSetOfTypeSetIDsMap =
      std::unordered_map<std::string, SetOfTypeSetIDs>;

  // Pass through topology API
  using node_iterator = GraphTopology::node_iterator;
  using edge_iterator = GraphTopology::edge_iterator;
  using edges_range = GraphTopology::edges_range;
  using iterator = GraphTopology::iterator;
  using Node = GraphTopology::Node;
  using Edge = GraphTopology::Edge;

private:
  /// Validate performs a sanity check on the the graph after loading
  Result<void> Validate();

  Result<void> DoWrite(
      tsuba::RDGHandle handle, const std::string& command_line,
      tsuba::RDG::RDGVersioningPolicy versioning_action);

  katana::Result<void> ConductWriteOp(
      const std::string& uri, const std::string& command_line,
      tsuba::RDG::RDGVersioningPolicy versioning_action);

  Result<void> WriteGraph(
      const std::string& uri, const std::string& command_line);

  Result<void> WriteView(
      const std::string& uri, const std::string& command_line);

  tsuba::RDG rdg_;
  std::unique_ptr<tsuba::RDGFile> file_;
  GraphTopology topology_;

  /// A map from the node TypeSetID to
  /// the set of the node type names it contains
  TypeSetIDToSetOfTypeNamesMap node_type_set_id_to_type_names_;
  /// A map from the edge TypeSetID to
  /// the set of the edge type names it contains
  TypeSetIDToSetOfTypeNamesMap edge_type_set_id_to_type_names_;

  /// A map from the node type name
  /// to the set of the node TypeSetIDs that contain it
  TypeNameToSetOfTypeSetIDsMap node_type_name_to_type_set_ids_;
  /// A map from the edge type name
  /// to the set of the edge TypeSetIDs that contain it
  TypeNameToSetOfTypeSetIDsMap edge_type_name_to_type_set_ids_;

  /// The node TypeSetID for each node in the graph
  katana::NUMAArray<TypeSetID> node_type_set_id_;
  /// The edge TypeSetID for each edge in the graph
  katana::NUMAArray<TypeSetID> edge_type_set_id_;

  // Keep partition_metadata, master_nodes, mirror_nodes out of the public interface,
  // while allowing Distribution to read/write it for RDG
  friend class Distribution;
  const tsuba::PartitionMetadata& partition_metadata() const {
    return rdg_.part_metadata();
  }

  void set_partition_metadata(const tsuba::PartitionMetadata& meta) {
    rdg_.set_part_metadata(meta);
  }

  void update_rdg_metadata(const std::string& part_policy, uint32_t num_hosts) {
    rdg_.set_view_name(fmt::format("rdg-{}-part{}", part_policy, num_hosts));
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

  /// Return the node property table for local nodes
  const std::shared_ptr<arrow::Table>& node_properties() const {
    return rdg_.node_properties();
  }

  /// Return the edge property table for local edges
  const std::shared_ptr<arrow::Table>& edge_properties() const {
    return rdg_.edge_properties();
  }

public:
  /// PropertyView provides a uniform interface when you don't need to
  /// distinguish operating on edge or node properties
  struct ReadOnlyPropertyView {
    const PropertyGraph* const_g;

    std::shared_ptr<arrow::Schema> (PropertyGraph::*schema_fn)() const;
    std::shared_ptr<arrow::ChunkedArray> (PropertyGraph::*property_fn_int)(
        int i) const;
    std::shared_ptr<arrow::ChunkedArray> (PropertyGraph::*property_fn_str)(
        const std::string& str) const;
    int32_t (PropertyGraph::*property_num_fn)() const;

    std::shared_ptr<arrow::Schema> schema() const {
      return (const_g->*schema_fn)();
    }

    std::shared_ptr<arrow::ChunkedArray> GetProperty(int i) const {
      return (const_g->*property_fn_int)(i);
    }

    std::shared_ptr<arrow::ChunkedArray> GetProperty(
        const std::string& str) const {
      return (const_g->*property_fn_str)(str);
    }

    int32_t GetNumProperties() const { return (const_g->*property_num_fn)(); }

    uint64_t ApproxMemUse() const {
      uint64_t total_mem_use = 0;
      for (int32_t i = 0; i < GetNumProperties(); ++i) {
        const auto& chunked_array = GetProperty(i);
        for (const auto& array : chunked_array->chunks()) {
          total_mem_use += katana::ApproxArrayMemUse(array);
        }
      }
      return total_mem_use;
    }
  };

  struct MutablePropertyView {
    ReadOnlyPropertyView ropv;
    PropertyGraph* g;

    Result<void> (PropertyGraph::*add_properties_fn)(
        const std::shared_ptr<arrow::Table>& props);
    Result<void> (PropertyGraph::*upsert_properties_fn)(
        const std::shared_ptr<arrow::Table>& props);
    Result<void> (PropertyGraph::*remove_property_int)(int i);
    Result<void> (PropertyGraph::*remove_property_str)(const std::string& str);

    std::shared_ptr<arrow::Schema> schema() const { return ropv.schema(); }

    std::shared_ptr<arrow::ChunkedArray> GetProperty(int i) const {
      return ropv.GetProperty(i);
    }

    std::shared_ptr<arrow::ChunkedArray> GetProperty(
        const std::string& str) const {
      return ropv.GetProperty(str);
    }

    int32_t GetNumProperties() const { return ropv.GetNumProperties(); }

    uint64_t ApproxMemUse() const { return ropv.ApproxMemUse(); }

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

  PropertyGraph() = default;

  PropertyGraph(
      std::unique_ptr<tsuba::RDGFile>&& rdg_file, tsuba::RDG&& rdg,
      GraphTopology&& topo) noexcept
      : rdg_(std::move(rdg)),
        file_(std::move(rdg_file)),
        topology_(std::move(topo)) {}

  PropertyGraph(katana::GraphTopology&& topo_to_assign) noexcept
      : rdg_(), file_(), topology_(std::move(topo_to_assign)) {}

  /// Make a property graph from a constructed RDG. Take ownership of the RDG
  /// and its underlying resources.
  static Result<std::unique_ptr<PropertyGraph>> Make(
      std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg);

  /// Make a property graph from an RDG name.
  static Result<std::unique_ptr<PropertyGraph>> Make(
      const std::string& rdg_name,
      const tsuba::RDGLoadOptions& opts = tsuba::RDGLoadOptions());

  /// Make a property graph from topology
  static Result<std::unique_ptr<PropertyGraph>> Make(
      GraphTopology&& topo_to_assign);

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

  /// Construct node & edge TypeSetIDs from node & edge properties
  /// Also constructs metadata to convert between types and TypeSetIDs
  /// Assumes all boolean or uint8 properties are types
  /// TODO(roshan) move this to be a part of Make()
  Result<void> ConstructTypeSetIDs();

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
  Result<void> WriteView(const std::string& command_line);
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

  /// \returns the number of node types
  size_t GetNodeTypesNum() const {
    return node_type_name_to_type_set_ids_.size();
  }

  /// \returns the number of edge types
  size_t GetEdgeTypesNum() const {
    return edge_type_name_to_type_set_ids_.size();
  }

  /// \returns true if a node type with @param name exists
  /// NB: no node may have this type
  /// TODO(roshan) build an index for the number of nodes with the type
  bool HasNodeType(const std::string& name) const {
    return node_type_name_to_type_set_ids_.count(name) == 1;
  }

  /// \returns true if an edge type with @param name exists
  /// NB: no edge may have this type
  /// TODO(roshan) build an index for the number of edges with the type
  bool HasEdgeType(const std::string& name) const {
    return edge_type_name_to_type_set_ids_.count(name) == 1;
  }

  /// \returns the set of node TypeSetIDs that contain
  /// the node type with @param name
  /// (assumes that the node type exists)
  const SetOfTypeSetIDs& NodeTypeNameToTypeSetIDs(
      const std::string& name) const {
    return node_type_name_to_type_set_ids_.at(name);
  }

  /// \returns the set of edge TypeSetIDs that contain
  /// the edge type with @param name
  /// (assumes that the edge type exists)
  const SetOfTypeSetIDs& EdgeTypeNameToTypeSetIDs(
      const std::string& name) const {
    return edge_type_name_to_type_set_ids_.at(name);
  }

  /// \returns the number of node TypeSetIDs (including kUnknownType)
  size_t GetNodeTypeSetIDsNum() const {
    return node_type_set_id_to_type_names_.size();
  }

  /// \returns the number of edge TypeSetIDs (including kUnknownType)
  size_t GetEdgeTypeSetIDsNum() const {
    return edge_type_set_id_to_type_names_.size();
  }

  /// \returns the set of node type names that contain
  /// the node TypeSetID @param node_type_set_id
  /// (assumes that the node TypeSetID exists)
  const SetOfTypeNames& NodeTypeSetIDToTypeNames(
      TypeSetID node_type_set_id) const {
    return node_type_set_id_to_type_names_.at(node_type_set_id);
  }

  /// \returns the set of edge type names that contain
  /// the edge TypeSetID @param edge_type_set_id
  /// (assumes that the edge TypeSetID exists)
  const SetOfTypeNames& EdgeTypeSetIDToTypeNames(
      TypeSetID edge_type_set_id) const {
    return edge_type_set_id_to_type_names_.at(edge_type_set_id);
  }

  /// \return returns the node TypeSetID for @param node
  TypeSetID GetNodeTypeSetID(Node node) const {
    return node_type_set_id_[node];
  }

  /// \return returns the edge TypeSetID for @param edge
  TypeSetID GetEdgeTypeSetID(Edge edge) const {
    return edge_type_set_id_[edge];
  }

  // Return type dictated by arrow
  /// Returns the number of node properties
  int32_t GetNumNodeProperties() const { return node_schema()->num_fields(); }

  /// Returns the number of edge properties
  int32_t GetNumEdgeProperties() const { return edge_schema()->num_fields(); }

  // num_rows() == num_nodes() (all local nodes)
  std::shared_ptr<arrow::ChunkedArray> GetNodeProperty(int i) const {
    if (i >= node_properties()->num_columns()) {
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

  /// \returns true if a node property/type with @param name exists
  bool HasNodeProperty(const std::string& name) const {
    return node_schema()->GetFieldIndex(name) != -1;
  }

  /// \returns true if an edge property/type with @param name exists
  bool HasEdgeProperty(const std::string& name) const {
    return edge_schema()->GetFieldIndex(name) != -1;
  }

  /// Get a node property by name.
  ///
  /// \param name The name of the property to get.
  /// \return The property data or NULL if the property is not found.
  std::shared_ptr<arrow::ChunkedArray> GetNodeProperty(
      const std::string& name) const {
    return node_properties()->GetColumnByName(name);
  }

  std::string GetNodePropertyName(int32_t i) const {
    return node_schema()->field(i)->name();
  }

  std::shared_ptr<arrow::ChunkedArray> GetEdgeProperty(
      const std::string& name) const {
    return edge_properties()->GetColumnByName(name);
  }

  std::string GetEdgePropertyName(int32_t i) const {
    return edge_schema()->field(i)->name();
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

  const GraphTopology& topology() const noexcept { return topology_; }

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

  /// Write a node property column out to storage and de-allocate the memory
  /// it was using
  Result<void> UnloadNodeProperty(int i);
  Result<void> UnloadNodeProperty(const std::string& prop_name);

  /// Write an edge property column out to storage and de-allocate the
  /// memory it was using
  Result<void> UnloadEdgeProperty(int i);
  Result<void> UnloadEdgeProperty(const std::string& prop_name);

  /// Load a node property by name put it in the table at index i
  /// if i is not a valid index, append the column to the end of the table
  Result<void> LoadNodeProperty(const std::string& name, int i = -1);

  /// Load an edge property by name put it in the table at index i
  /// if i is not a valid index, append the column to the end of the table
  Result<void> LoadEdgeProperty(const std::string& name, int i = -1);

  /// Load a node property by name if it is absent and append its column to
  /// the table do nothing otherwise
  Result<void> EnsureNodePropertyLoaded(const std::string& name);

  /// Load an edge property by name if it is absent and append its column to
  /// the table do nothing otherwise
  Result<void> EnsureEdgePropertyLoaded(const std::string& name);

  std::vector<std::string> ListNodeProperties() const;
  std::vector<std::string> ListEdgeProperties() const;

  /// Remove all node properties
  void DropNodeProperties() { rdg_.DropNodeProperties(); }
  /// Remove all edge properties
  void DropEdgeProperties() { rdg_.DropEdgeProperties(); }

  MutablePropertyView NodeMutablePropertyView() {
    return MutablePropertyView{
        .ropv =
            {
                .const_g = this,
                .schema_fn = &PropertyGraph::node_schema,
                .property_fn_int = &PropertyGraph::GetNodeProperty,
                .property_fn_str = &PropertyGraph::GetNodeProperty,
                .property_num_fn = &PropertyGraph::GetNumNodeProperties,
            },
        .g = this,
        .add_properties_fn = &PropertyGraph::AddNodeProperties,
        .upsert_properties_fn = &PropertyGraph::UpsertNodeProperties,
        .remove_property_int = &PropertyGraph::RemoveNodeProperty,
        .remove_property_str = &PropertyGraph::RemoveNodeProperty,
    };
  }
  ReadOnlyPropertyView NodeReadOnlyPropertyView() const {
    return ReadOnlyPropertyView{
        .const_g = this,
        .schema_fn = &PropertyGraph::node_schema,
        .property_fn_int = &PropertyGraph::GetNodeProperty,
        .property_fn_str = &PropertyGraph::GetNodeProperty,
        .property_num_fn = &PropertyGraph::GetNumNodeProperties,
    };
  }

  MutablePropertyView EdgeMutablePropertyView() {
    return MutablePropertyView{
        .ropv =
            {
                .const_g = this,
                .schema_fn = &PropertyGraph::edge_schema,
                .property_fn_int = &PropertyGraph::GetEdgeProperty,
                .property_fn_str = &PropertyGraph::GetEdgeProperty,
                .property_num_fn = &PropertyGraph::GetNumEdgeProperties,
            },
        .g = this,
        .add_properties_fn = &PropertyGraph::AddEdgeProperties,
        .upsert_properties_fn = &PropertyGraph::UpsertEdgeProperties,
        .remove_property_int = &PropertyGraph::RemoveEdgeProperty,
        .remove_property_str = &PropertyGraph::RemoveEdgeProperty,
    };
  }
  ReadOnlyPropertyView EdgeReadOnlyPropertyView() const {
    return ReadOnlyPropertyView{
        .const_g = this,
        .schema_fn = &PropertyGraph::edge_schema,
        .property_fn_int = &PropertyGraph::GetEdgeProperty,
        .property_fn_str = &PropertyGraph::GetEdgeProperty,
        .property_num_fn = &PropertyGraph::GetNumEdgeProperties,
    };
  }

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
/// indices to the new indices) which results due to  sorting.
KATANA_EXPORT Result<std::unique_ptr<katana::NUMAArray<uint64_t>>>
SortAllEdgesByDest(PropertyGraph* pg);

/// FindEdgeSortedByDest finds the "node_to_find" id in the
/// sorted edgelist of the "node" using binary search.
///
/// This returns the matched edge index if 'node_to_find' is present
/// in the edgelist of 'node' else edge end if 'node_to_find' is not found.
// TODO(amber): make this a method of a sorted topology class in the near future
// TODO(amber): this method should return an edge_iterator
KATANA_EXPORT GraphTopology::Edge FindEdgeSortedByDest(
    const PropertyGraph* graph, GraphTopology::Node node,
    GraphTopology::Node node_to_find);

/// Relabel all nodes in the graph by sorting in the descending
/// order by node degree.
// TODO(amber): this method should return a new sorted topology
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
// TODO(amber): this function should return a new topology
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
/// \param topology The original property graph topology
/// \return The new transposed property graph by reversing the edges
// TODO(lhc): hack for bfs-direct-opt
// TODO(amber): this function should return a new topology
KATANA_EXPORT Result<std::unique_ptr<PropertyGraph>>
CreateTransposeGraphTopology(const GraphTopology& topology);

}  // namespace katana

#endif

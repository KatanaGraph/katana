#ifndef KATANA_LIBGALOIS_KATANA_PROPERTYGRAPH_H_
#define KATANA_LIBGALOIS_KATANA_PROPERTYGRAPH_H_

#include <utility>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <arrow/type_traits.h>

#include "katana/ArrowInterchange.h"
#include "katana/Details.h"
#include "katana/EntityTypeManager.h"
#include "katana/ErrorCode.h"
#include "katana/Iterators.h"
#include "katana/NUMAArray.h"
#include "katana/PropertyIndex.h"
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

/// Types used by all topologies
struct KATANA_EXPORT GraphTopologyTypes {
  using Node = uint32_t;
  using Edge = uint64_t;
  using node_iterator = boost::counting_iterator<Node>;
  using edge_iterator = boost::counting_iterator<Edge>;
  using nodes_range = StandardRange<node_iterator>;
  using edges_range = StandardRange<edge_iterator>;
  using iterator = node_iterator;
};

class KATANA_EXPORT EdgeShuffleTopology;
class KATANA_EXPORT EdgeTypeAwareTopology;

/// A graph topology represents the adjacency information for a graph in CSR
/// format.
class KATANA_EXPORT GraphTopology : public GraphTopologyTypes {
public:
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

  /// Checks equality against another instance of GraphTopology.
  /// WARNING: Expensive operation due to element-wise checks on large arrays
  /// @param that: GraphTopology instance to compare against
  /// @returns true if topology arrays are equal
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

  /// Gets the edge range of some node.
  ///
  /// \param node node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range edges(Node node) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(node <= adj_indices_.size());
    edge_iterator e_beg{node > 0 ? adj_indices_[node - 1] : 0};
    edge_iterator e_end{adj_indices_[node]};

    return MakeStandardRange(e_beg, e_end);
  }

  Node edge_dest(Edge edge_id) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(edge_id < dests_.size());
    return dests_[edge_id];
  }

  nodes_range nodes(Node begin, Node end) const noexcept {
    return MakeStandardRange<node_iterator>(begin, end);
  }

  nodes_range all_nodes() const noexcept {
    return nodes(Node{0}, static_cast<Node>(num_nodes()));
  }

  edges_range all_edges() const noexcept {
    return MakeStandardRange<edge_iterator>(Edge{0}, Edge{num_edges()});
  }
  // Standard container concepts

  node_iterator begin() const noexcept { return node_iterator(0); }

  node_iterator end() const noexcept { return node_iterator(num_nodes()); }

  size_t size() const noexcept { return num_nodes(); }

  bool empty() const noexcept { return num_nodes() == 0; }

  ///@param node node to get degree for
  ///@returns Degree of node N
  size_t degree(Node node) const noexcept { return edges(node).size(); }

  Edge original_edge_id(const Edge& eid) const noexcept { return eid; }

  Node original_node_id(const Node& nid) const noexcept { return nid; }

private:
  // need these friend relationships to construct instances of friend classes below
  // by moving NUMAArrays in this class.
  friend class EdgeShuffleTopology;
  friend class EdgeTypeAwareTopology;

  NUMAArray<Edge>& GetAdjIndices() noexcept { return adj_indices_; }
  NUMAArray<Node>& GetDests() noexcept { return dests_; }

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

  /// Manages the relations between the node entity types
  EntityTypeManager node_entity_type_manager_;

  /// Manages the relations between the edge entity types
  EntityTypeManager edge_entity_type_manager_;

  /// The node EntityTypeID for each node's most specific type
  katana::NUMAArray<EntityTypeID> node_entity_type_id_;
  /// The edge EntityTypeID for each edge's most specific type
  katana::NUMAArray<EntityTypeID> edge_entity_type_id_;

  // List of node and edge indexes on this graph.
  std::vector<std::unique_ptr<PropertyIndex<GraphTopology::Node>>>
      node_indexes_;
  std::vector<std::unique_ptr<PropertyIndex<GraphTopology::Edge>>>
      edge_indexes_;

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

    std::shared_ptr<arrow::Schema> (PropertyGraph::*loaded_schema_fn)() const;
    std::shared_ptr<arrow::Schema> (PropertyGraph::*full_schema_fn)() const;
    std::shared_ptr<arrow::ChunkedArray> (PropertyGraph::*property_fn_int)(
        int i) const;
    std::shared_ptr<arrow::ChunkedArray> (PropertyGraph::*property_fn_str)(
        const std::string& str) const;
    int32_t (PropertyGraph::*property_num_fn)() const;

    std::shared_ptr<arrow::Schema> loaded_schema() const {
      return (const_g->*loaded_schema_fn)();
    }

    std::shared_ptr<arrow::Schema> full_schema() const {
      return (const_g->*full_schema_fn)();
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

    std::shared_ptr<arrow::Schema> loaded_schema() const {
      return ropv.loaded_schema();
    }
    std::shared_ptr<arrow::Schema> full_schema() const {
      return ropv.full_schema();
    }

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

  PropertyGraph(
      katana::GraphTopology&& topo_to_assign,
      NUMAArray<EntityTypeID>&& node_entity_type_id,
      NUMAArray<EntityTypeID>&& edge_entity_type_id,
      EntityTypeManager&& node_type_manager,
      EntityTypeManager&& edge_type_manager) noexcept
      : rdg_(),
        file_(),
        topology_(std::move(topo_to_assign)),
        node_entity_type_manager_(std::move(node_type_manager)),
        edge_entity_type_manager_(std::move(edge_type_manager)),
        node_entity_type_id_(std::move(node_entity_type_id)),
        edge_entity_type_id_(std::move(edge_entity_type_id)) {}

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

  /// Make a property graph from topology and type arrays
  static Result<std::unique_ptr<PropertyGraph>> Make(
      GraphTopology&& topo_to_assign,
      NUMAArray<EntityTypeID>&& node_entity_type_id,
      NUMAArray<EntityTypeID>&& edge_entity_type_id,
      EntityTypeManager&& node_type_manager,
      EntityTypeManager&& edge_type_manager);

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

  /// Construct node & edge EntityTypeIDs from node & edge properties
  /// Also constructs metadata to convert between atomic types and EntityTypeIDs
  /// Assumes all boolean or uint8 properties are atomic types
  /// TODO(roshan) move this to be a part of Make()
  Result<void> ConstructEntityTypeIDs();

  /// This is an unfortunate hack. Due to some technical debt, we need a way to
  /// modify these arrays in place from outside this class. This style mirrors a
  /// similar hack in GraphTopology and hopefully makes it clear that these
  /// functions should not be used lightly.
  const EntityTypeID* node_type_data() const noexcept {
    return node_entity_type_id_.data();
  }
  /// This is an unfortunate hack. Due to some technical debt, we need a way to
  /// modify these arrays in place from outside this class. This style mirrors a
  /// similar hack in GraphTopology and hopefully makes it clear that these
  /// functions should not be used lightly.
  const EntityTypeID* edge_type_data() const noexcept {
    return edge_entity_type_id_.data();
  }

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

  /// get the schema for loaded node properties
  std::shared_ptr<arrow::Schema> loaded_node_schema() const {
    return node_properties()->schema();
  }

  /// get the schema for all node properties (includes unloaded properties)
  std::shared_ptr<arrow::Schema> full_node_schema() const {
    return rdg_.full_node_schema();
  }

  /// get the schema for loaded edge properties
  std::shared_ptr<arrow::Schema> loaded_edge_schema() const {
    return edge_properties()->schema();
  }

  /// get the schema for all edge properties (includes unloaded properties)
  std::shared_ptr<arrow::Schema> full_edge_schema() const {
    return rdg_.full_edge_schema();
  }

  /// \returns the number of node atomic types
  size_t GetNumNodeAtomicTypes() const {
    return node_entity_type_manager_.GetNumAtomicTypes();
  }

  /// \returns the number of edge atomic types
  size_t GetNumEdgeAtomicTypes() const {
    return edge_entity_type_manager_.GetNumAtomicTypes();
  }

  /// \returns the number of node entity types (including kUnknownEntityType)
  size_t GetNumNodeEntityTypes() const {
    return node_entity_type_manager_.GetNumEntityTypes();
  }

  /// \returns the number of edge entity types (including kUnknownEntityType)
  size_t GetNumEdgeEntityTypes() const {
    return edge_entity_type_manager_.GetNumEntityTypes();
  }

  /// \returns true iff a node atomic type @param name exists
  /// NB: no node may have a type that intersects with this atomic type
  /// TODO(roshan) build an index for the number of nodes with the type
  bool HasAtomicNodeType(const std::string& name) const {
    return node_entity_type_manager_.HasAtomicType(name);
  }

  /// \returns true iff an edge atomic type with @param name exists
  /// NB: no edge may have a type that intersects with this atomic type
  /// TODO(roshan) build an index for the number of edges with the type
  bool HasAtomicEdgeType(const std::string& name) const {
    return edge_entity_type_manager_.HasAtomicType(name);
  }

  /// \returns true iff a node entity type @param node_entity_type_id exists
  /// NB: even if it exists, it may not be the most specific type for any node
  /// (returns true for kUnknownEntityType)
  bool HasNodeEntityType(EntityTypeID node_entity_type_id) const {
    return node_entity_type_manager_.HasEntityType(node_entity_type_id);
  }

  /// \returns true iff an edge entity type @param node_entity_type_id exists
  /// NB: even if it exists, it may not be the most specific type for any edge
  /// (returns true for kUnknownEntityType)
  bool HasEdgeEntityType(EntityTypeID edge_entity_type_id) const {
    return edge_entity_type_manager_.HasEntityType(edge_entity_type_id);
  }

  /// \returns the node EntityTypeID for an atomic node type with name
  /// @param name
  /// (assumes that the node type exists)
  EntityTypeID GetNodeEntityTypeID(const std::string& name) const {
    return node_entity_type_manager_.GetEntityTypeID(name);
  }

  /// \returns the edge EntityTypeID for an atomic edge type with name
  /// @param name
  /// (assumes that the edge type exists)
  EntityTypeID GetEdgeEntityTypeID(const std::string& name) const {
    return edge_entity_type_manager_.GetEntityTypeID(name);
  }

  /// \returns the name of the atomic type if the node EntityTypeID
  /// @param node_entity_type_id is an atomic type,
  /// nullopt otherwise
  std::optional<std::string> GetNodeAtomicTypeName(
      EntityTypeID node_entity_type_id) const {
    return node_entity_type_manager_.GetAtomicTypeName(node_entity_type_id);
  }

  /// \returns the name of the atomic type if the edge EntityTypeID
  /// @param edge_entity_type_id is an atomic type,
  /// nullopt otherwise
  std::optional<std::string> GetEdgeAtomicTypeName(
      EntityTypeID edge_entity_type_id) const {
    return edge_entity_type_manager_.GetAtomicTypeName(edge_entity_type_id);
  }

  /// \returns the set of node entity types that intersect
  /// the node atomic type @param node_entity_type_id
  /// (assumes that the node atomic type exists)
  const SetOfEntityTypeIDs& GetNodeSupertypes(
      EntityTypeID node_entity_type_id) const {
    return node_entity_type_manager_.GetSupertypes(node_entity_type_id);
  }

  /// \returns the set of edge entity types that intersect
  /// the edge atomic type @param edge_entity_type_id
  /// (assumes that the edge atomic type exists)
  const SetOfEntityTypeIDs& GetEdgeSupertypes(
      EntityTypeID edge_entity_type_id) const {
    return edge_entity_type_manager_.GetSupertypes(edge_entity_type_id);
  }

  /// \returns the set of atomic node types that are intersected
  /// by the node entity type @param node_entity_type_id
  /// (assumes that the node entity type exists)
  const SetOfEntityTypeIDs& GetNodeAtomicSubtypes(
      EntityTypeID node_entity_type_id) const {
    return node_entity_type_manager_.GetAtomicSubtypes(node_entity_type_id);
  }

  /// \returns the set of atomic edge types that are intersected
  /// by the edge entity type @param edge_entity_type_id
  /// (assumes that the edge entity type exists)
  const SetOfEntityTypeIDs& GetEdgeAtomicSubtypes(
      EntityTypeID edge_entity_type_id) const {
    return edge_entity_type_manager_.GetAtomicSubtypes(edge_entity_type_id);
  }

  /// \returns true iff the node type @param sub_type is a
  /// sub-type of the node type @param super_type
  /// (assumes that the sub_type and super_type EntityTypeIDs exists)
  bool IsNodeSubtypeOf(EntityTypeID sub_type, EntityTypeID super_type) const {
    return node_entity_type_manager_.IsSubtypeOf(sub_type, super_type);
  }

  /// \returns true iff the edge type @param sub_type is a
  /// sub-type of the edge type @param super_type
  /// (assumes that the sub_type and super_type EntityTypeIDs exists)
  bool IsEdgeSubtypeOf(EntityTypeID sub_type, EntityTypeID super_type) const {
    return edge_entity_type_manager_.IsSubtypeOf(sub_type, super_type);
  }

  /// \return returns the most specific node entity type for @param node
  EntityTypeID GetTypeOfNode(Node node) const {
    return node_entity_type_id_[node];
  }

  /// \return returns the most specific edge entity type for @param edge
  EntityTypeID GetTypeOfEdge(Edge edge) const {
    return edge_entity_type_id_[edge];
  }

  /// \return true iff the node @param node has the given entity type
  /// @param node_entity_type_id (need not be the most specific type)
  /// (assumes that the node entity type exists)
  bool DoesNodeHaveType(Node node, EntityTypeID node_entity_type_id) const {
    return IsNodeSubtypeOf(node_entity_type_id, GetTypeOfNode(node));
  }

  /// \return true iff the edge @param edge has the given entity type
  /// @param edge_entity_type_id (need not be the most specific type)
  /// (assumes that the edge entity type exists)
  bool DoesEdgeHaveType(Edge edge, EntityTypeID edge_entity_type_id) const {
    return IsEdgeSubtypeOf(edge_entity_type_id, GetTypeOfEdge(edge));
  }

  // Return type dictated by arrow
  /// Returns the number of node properties
  int32_t GetNumNodeProperties() const {
    return loaded_node_schema()->num_fields();
  }

  /// Returns the number of edge properties
  int32_t GetNumEdgeProperties() const {
    return loaded_edge_schema()->num_fields();
  }

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
    return loaded_node_schema()->GetFieldIndex(name) != -1;
  }

  /// \returns true if an edge property/type with @param name exists
  bool HasEdgeProperty(const std::string& name) const {
    return loaded_edge_schema()->GetFieldIndex(name) != -1;
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
    return loaded_node_schema()->field(i)->name();
  }

  std::shared_ptr<arrow::ChunkedArray> GetEdgeProperty(
      const std::string& name) const {
    return edge_properties()->GetColumnByName(name);
  }

  std::string GetEdgePropertyName(int32_t i) const {
    return loaded_edge_schema()->field(i)->name();
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
                .loaded_schema_fn = &PropertyGraph::loaded_node_schema,
                .full_schema_fn = &PropertyGraph::full_node_schema,
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
        .loaded_schema_fn = &PropertyGraph::loaded_node_schema,
        .full_schema_fn = &PropertyGraph::full_node_schema,
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
                .loaded_schema_fn = &PropertyGraph::loaded_edge_schema,
                .full_schema_fn = &PropertyGraph::full_edge_schema,
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
        .loaded_schema_fn = &PropertyGraph::loaded_edge_schema,
        .full_schema_fn = &PropertyGraph::full_edge_schema,
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

  /// Gets the destination for an edge.
  ///
  /// @param edge edge iterator to get the destination of
  /// @returns node iterator to the edge destination
  node_iterator GetEdgeDest(const edge_iterator& edge) const {
    auto node_id = topology().edge_dest(*edge);
    return node_iterator(node_id);
  }

  // Creates an index over a node property.
  Result<void> MakeNodeIndex(const std::string& column_name);

  // Creates an index over an edge property.
  Result<void> MakeEdgeIndex(const std::string& column_name);

  // Returns the list of node indexes.
  const std::vector<std::unique_ptr<PropertyIndex<GraphTopology::Node>>>&
  node_indexes() const {
    return node_indexes_;
  }

  // Returns the list of edge indexes.
  const std::vector<std::unique_ptr<PropertyIndex<GraphTopology::Edge>>>&
  edge_indexes() const {
    return edge_indexes_;
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

/// Renumber all nodes in the graph by sorting in the descending
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

class KATANA_EXPORT EdgeTypeIndex : public GraphTopologyTypes {
  using EdgeTypeInternal = EntityTypeID;
  /// map an integer id to each unique edge edge_type in the graph, such that, the
  /// integer ids assigned are contiguous, i.e., 0 .. num_unique_types-1
  using EdgeTypeIDToIndexMap = std::unordered_map<EdgeTypeInternal, uint32_t>;
  /// reverse map that allows looking up edge_type using its integer index
  using EdgeIndexToTypeIDMap = std::vector<EdgeTypeInternal>;

public:
  using EdgeTypeID = EdgeTypeInternal;
  using EdgeTypeIDRange =
      katana::StandardRange<EdgeIndexToTypeIDMap::const_iterator>;

  EdgeTypeIndex() = default;
  EdgeTypeIndex(EdgeTypeIndex&&) = default;
  EdgeTypeIndex& operator=(EdgeTypeIndex&&) = default;

  EdgeTypeIndex(const EdgeTypeIndex&) = delete;
  EdgeTypeIndex& operator=(const EdgeTypeIndex&) = delete;

  static EdgeTypeIndex Make(const PropertyGraph* pg) noexcept;

  EdgeTypeID GetType(uint32_t index) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(size_t(index) < edge_index_to_type_map_.size());
    return edge_index_to_type_map_[index];
  }

  uint32_t GetIndex(const EdgeTypeID& edge_type) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(edge_type_to_index_map_.count(edge_type) > 0);
    return edge_type_to_index_map_.at(edge_type);
  }

  size_t num_unique_types() const noexcept {
    return edge_index_to_type_map_.size();
  }

  /// @param edge_type: edge_type to check
  /// @returns true iff there exists some edge in the graph with that edge_type
  bool has_edge_type_id(const EdgeTypeID& edge_type) const noexcept {
    return (
        edge_type_to_index_map_.find(edge_type) !=
        edge_type_to_index_map_.cend());
  }

  /// Wrapper to get the distinct edge types in the graph.
  ///
  /// @returns Range of the distinct edge types
  EdgeTypeIDRange distinct_edge_type_ids() const noexcept {
    return EdgeTypeIDRange{
        edge_index_to_type_map_.cbegin(), edge_index_to_type_map_.cend()};
  }

private:
  EdgeTypeIndex(
      EdgeTypeIDToIndexMap&& edge_type_to_index,
      EdgeIndexToTypeIDMap&& edge_index_to_type) noexcept
      : edge_type_to_index_map_(std::move(edge_type_to_index)),
        edge_index_to_type_map_(std::move(edge_index_to_type)) {
    KATANA_LOG_ASSERT(
        edge_index_to_type_map_.size() == edge_type_to_index_map_.size());
  }

  EdgeTypeIDToIndexMap edge_type_to_index_map_;
  EdgeIndexToTypeIDMap edge_index_to_type_map_;
};

// TODO(amber): make OrigEdgeIDMap optional via template argument
class KATANA_EXPORT EdgeShuffleTopology : public GraphTopology {
  using Base = GraphTopology;
  using OrigEdgeIDMap = katana::NUMAArray<Edge>;
  using EdgeTypeID = EntityTypeID;

public:
  EdgeShuffleTopology() = default;
  EdgeShuffleTopology(EdgeShuffleTopology&&) = default;
  EdgeShuffleTopology& operator=(EdgeShuffleTopology&&) = default;

  EdgeShuffleTopology(const EdgeShuffleTopology&) = delete;
  EdgeShuffleTopology& operator=(const EdgeShuffleTopology&) = delete;

  static EdgeShuffleTopology MakeTransposeCopy(const PropertyGraph* pg);
  static EdgeShuffleTopology MakeOriginalCopy(const PropertyGraph* pg);

  auto original_edge_id(Edge eid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(eid < orig_edge_ids_.size());
    return orig_edge_ids_[eid];
  }

  void SortEdgesByTypeThenDest() noexcept;

private:
  NUMAArray<Edge>& GetOrigEdgeIDs() noexcept { return orig_edge_ids_; }

  // Needed for creating EdgeTypeAwareTopology from EdgeShuffleTopology using
  // move semantics on member NUMAArrays below
  friend class EdgeTypeAwareTopology;

  EdgeShuffleTopology(
      const PropertyGraph* pg, NUMAArray<Edge>&& adj_indices,
      NUMAArray<Node>&& dests, NUMAArray<Edge>&& orig_edge_ids) noexcept
      : GraphTopology(std::move(adj_indices), std::move(dests)),
        prop_graph_(pg),
        orig_edge_ids_(std::move(orig_edge_ids)) {}

  const PropertyGraph* prop_graph_;
  OrigEdgeIDMap orig_edge_ids_;
};

// TODO(amber): make private
template <typename Topo>
struct EdgeDestComparator {
  const Topo* topo_;

  bool operator()(const typename Topo::Edge& e, const typename Topo::Node& n)
      const noexcept {
    return topo_->edge_dest(e) < n;
  }

  bool operator()(const typename Topo::Node& n, const typename Topo::Edge& e)
      const noexcept {
    return n < topo_->edge_dest(e);
  }
};

/// store adjacency indices per each node such that they are divided by edge edge_type type.
/// Requires sorting the graph by edge edge_type type
class KATANA_EXPORT EdgeTypeAwareTopology : public GraphTopologyTypes {
  // TODO(amber): add a template flag to choose between copying edge types from
  // PropertyGraph vs accessing the data in PropertyGraph through
  // orig_edge_ids_

protected:
  using EdgeTypeID = EntityTypeID;

public:
  EdgeTypeAwareTopology() = default;
  EdgeTypeAwareTopology(EdgeTypeAwareTopology&&) = default;
  EdgeTypeAwareTopology& operator=(EdgeTypeAwareTopology&&) = default;

  EdgeTypeAwareTopology(const EdgeTypeAwareTopology&) = delete;
  EdgeTypeAwareTopology& operator=(const EdgeTypeAwareTopology&) = delete;

  static EdgeTypeAwareTopology MakeFromDefaultTopology(
      const PropertyGraph* pg, const EdgeTypeIndex* edge_type_index);
  static EdgeTypeAwareTopology MakeFromTransposeTopology(
      const PropertyGraph* pg, const EdgeTypeIndex* edge_type_index);

  uint64_t num_nodes() const noexcept {
    // corner case: graph with 0 edges
    if (edge_type_index_->num_unique_types() == 0) {
      KATANA_LOG_DEBUG_ASSERT(num_edges() == 0);
      return adj_indices_.size();
    }
    return adj_indices_.size() / edge_type_index_->num_unique_types();
  }

  uint64_t num_edges() const noexcept { return dests_.size(); }

  // Edge accessors

  /// @param N node to get edges for
  /// @param edge_type edge_type to get edges of
  /// @returns Range to edges of node N that have edge type == edge_type
  edges_range edges(Node N, const EdgeTypeID& edge_type) const noexcept {
    // adj_indices_ is expanded so that it stores P prefix sums per node, where
    // P == edge_type_index_->num_unique_types()
    // We pick the prefix sum based on the index of the edge_type provided
    auto beg_idx = (N * edge_type_index_->num_unique_types()) +
                   edge_type_index_->GetIndex(edge_type);
    edge_iterator e_beg{(beg_idx == 0) ? 0 : adj_indices_[beg_idx - 1]};

    auto end_idx = (N * edge_type_index_->num_unique_types()) +
                   edge_type_index_->GetIndex(edge_type);
    KATANA_LOG_DEBUG_ASSERT(end_idx < adj_indices_.size());
    edge_iterator e_end{adj_indices_[end_idx]};

    return katana::MakeStandardRange(e_beg, e_end);
  }

  /// Gets the edge range of some node.
  ///
  /// \param node node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range edges(Node N) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(N < num_nodes());

    auto beg_idx = N * edge_type_index_->num_unique_types();
    KATANA_LOG_DEBUG_ASSERT(beg_idx <= adj_indices_.size());
    edge_iterator e_beg{beg_idx > 0 ? adj_indices_[beg_idx - 1] : 0};

    auto end_idx = (N + 1) * edge_type_index_->num_unique_types();
    KATANA_LOG_DEBUG_ASSERT(end_idx <= adj_indices_.size());
    // end_idx == 0 means num_unique_types() returns 0, which means either
    // edge_type_index_ wasn't properly initialized or graph has no edges
    if (end_idx == 0) {
      KATANA_LOG_DEBUG_ASSERT(num_edges() == 0);
    }
    edge_iterator e_end{end_idx > 0 ? adj_indices_[end_idx - 1] : 0};

    return MakeStandardRange(e_beg, e_end);
  }

  Node edge_dest(Edge edge_id) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(edge_id < dests_.size());
    return dests_[edge_id];
  }

  /// @param node node to get degree for
  /// @returns Degree of node N
  size_t degree(Node node) const noexcept { return edges(node).size(); }

  /// @param N node to get degree for
  /// @param edge_type edge_type to get degree of
  /// @returns Degree of node N
  size_t degree(Node N, const EdgeTypeID& edge_type) const noexcept {
    return edges(N, edge_type).size();
  }

  nodes_range nodes(Node begin, Node end) const noexcept {
    return MakeStandardRange<node_iterator>(begin, end);
  }

  nodes_range all_nodes() const noexcept {
    return nodes(Node{0}, static_cast<Node>(num_nodes()));
  }

  edges_range all_edges() const noexcept {
    return MakeStandardRange<edge_iterator>(Edge{0}, Edge{num_edges()});
  }
  // Standard container concepts

  node_iterator begin() const noexcept { return node_iterator(0); }

  node_iterator end() const noexcept { return node_iterator(num_nodes()); }

  size_t size() const noexcept { return num_nodes(); }

  bool empty() const noexcept { return num_nodes() == 0; }

  Edge original_edge_id(const Edge& e) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(e < num_edges());
    return orig_edge_ids_[e];
  }

  Node original_node_id(const Node& nid) const noexcept { return nid; }

  auto GetDistinctEdgeTypes() const noexcept {
    return edge_type_index_->distinct_edge_type_ids();
  }

  bool DoesEdgeTypeExist(const EdgeTypeID& edge_type) const noexcept {
    return edge_type_index_->has_edge_type_id(edge_type);
  }

  /// Returns all edges from src to dst with some edge_type.  If not found, returns
  /// empty range.
  edges_range FindAllEdgesWithType(
      Node node, Node key, const EdgeTypeID& edge_type) const noexcept {
    auto e_range = edges(node, edge_type);
    if (e_range.empty()) {
      return e_range;
    }

    EdgeDestComparator<EdgeTypeAwareTopology> comp{this};
    auto [first_it, last_it] =
        std::equal_range(e_range.begin(), e_range.end(), key, comp);

    if (first_it == e_range.end() || edge_dest(*first_it) != key) {
      // return empty range
      return MakeStandardRange(e_range.end(), e_range.end());
    }

    auto ret_range = MakeStandardRange(first_it, last_it);
    for ([[maybe_unused]] auto e : ret_range) {
      KATANA_LOG_DEBUG_ASSERT(edge_dest(e) == key);
    }
    return ret_range;
  }

  /// Returns an edge iterator to an edge with some node and key by
  /// searching for the key via the node's outgoing or incoming edges.
  /// If not found, returns nothing.
  // TODO(amber): Assess the usefulness of this method. This method cannot return
  // edges of all types. Only the first found type. We should however support
  // find_edges(src, dst) or find_edge(src, dst) that doesn't care about edge type
  edges_range FindAllEdgesSingleType(Node src, Node dst) const {
    // trivial check; can't be connected if degree is 0

    auto empty_range = MakeStandardRange<edge_iterator>(Edge{0}, Edge{0});
    if (degree(src) == 0) {
      return empty_range;
    }

    // loop through all type_ids
    for (const EdgeTypeID& edge_type : GetDistinctEdgeTypes()) {
      // always use out edges (we want an id to the out edge returned)
      edges_range r = FindAllEdgesWithType(src, dst, edge_type);

      // return if something was found
      if (r) {
        return r;
      }
    }

    // not found, return empty optional
    return empty_range;
  }

  /// Check if vertex src is connected to vertex dst with the given edge edge_type
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @param edge_type edge_type of the edge
  /// @returns true iff the edge exists
  bool IsConnectedWithEdgeType(
      Node src, Node dst, const EdgeTypeID& edge_type) const {
    auto e_range = edges(src, edge_type);
    if (e_range.empty()) {
      return false;
    }

    EdgeDestComparator<EdgeTypeAwareTopology> comp{this};
    return std::binary_search(e_range.begin(), e_range.end(), dst, comp);
  }

  /// Check if vertex src is connected to vertex dst with any edge edge_type
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @returns true iff the edge exists
  bool IsConnected(Node src, Node dst) const {
    // trivial check; can't be connected if degree is 0

    if (degree(src) == 0ul) {
      return false;
    }

    for (const auto& edge_type : GetDistinctEdgeTypes()) {
      if (IsConnectedWithEdgeType(src, dst, edge_type)) {
        return true;
      }
    }
    return false;
  }

private:
  using AdjIndexArray = katana::NUMAArray<Edge>;
  using EdgeDstArray = katana::NUMAArray<Node>;
  using OrigEdgeIDMap = katana::NUMAArray<Edge>;

  // Must invoke SortAllEdgesByDataThenDst() before
  // calling this function
  static AdjIndexArray CreatePerEdgeTypeAdjacencyIndex(
      const PropertyGraph* pg, const EdgeTypeIndex* edge_type_index,
      const EdgeShuffleTopology& topo) noexcept;

  EdgeTypeAwareTopology(
      const PropertyGraph* pg, const EdgeTypeIndex* edge_type_index,
      NUMAArray<Edge>&& adj_indices, NUMAArray<Node>&& dests,
      NUMAArray<Edge>&& orig_edge_ids) noexcept
      : edge_type_index_(edge_type_index),
        adj_indices_(std::move(adj_indices)),
        dests_(std::move(dests)),
        orig_edge_ids_(std::move(orig_edge_ids)) {
    KATANA_LOG_ASSERT(pg);
    KATANA_LOG_DEBUG_ASSERT(edge_type_index);

    KATANA_LOG_DEBUG_ASSERT(
        adj_indices_.size() ==
        pg->topology().num_nodes() * edge_type_index_->num_unique_types());

    KATANA_LOG_DEBUG_ASSERT(dests_.size() == pg->topology().num_edges());
    KATANA_LOG_DEBUG_ASSERT(dests_.size() == orig_edge_ids_.size());
  }

  const EdgeTypeIndex* edge_type_index_;
  AdjIndexArray adj_indices_;
  EdgeDstArray dests_;
  OrigEdgeIDMap orig_edge_ids_;
};

/// Provides both out-going and in-coming topology API
/// Provides Edge Type Aware topology API
class KATANA_EXPORT EdgeTypeAwareBiDirTopology : public EdgeTypeAwareTopology {
  // Inheriting to reuse outgoing topology API
  //
  using OutTopoBase = EdgeTypeAwareTopology;

  using OutTopoBase::EdgeTypeID;

public:
  static EdgeTypeAwareBiDirTopology Make(const PropertyGraph* pg) noexcept {
    auto edge_type_index =
        std::make_unique<EdgeTypeIndex>(EdgeTypeIndex::Make(pg));
    return EdgeTypeAwareBiDirTopology{pg, std::move(edge_type_index)};
  }

  EdgeTypeAwareBiDirTopology() = default;
  EdgeTypeAwareBiDirTopology(EdgeTypeAwareBiDirTopology&&) = default;
  EdgeTypeAwareBiDirTopology& operator=(EdgeTypeAwareBiDirTopology&&) = default;

  EdgeTypeAwareBiDirTopology(const EdgeTypeAwareBiDirTopology&) = delete;
  EdgeTypeAwareBiDirTopology& operator=(const EdgeTypeAwareBiDirTopology&) =
      delete;

  bool has_edge_type_id(const EdgeTypeID& edge_type) const noexcept {
    return edge_type_index_->has_edge_type_id(edge_type);
  }

  auto distinct_edge_type_ids() const noexcept {
    return edge_type_index_->distinct_edge_type_ids();
  }

  edges_range in_edges(Node N) const noexcept { return in_topo_.edges(N); }

  edges_range in_edges(Node N, const EdgeTypeID& edge_type) const noexcept {
    return in_topo_.edges(N, edge_type);
  }

  Node in_edge_dest(Edge edge_id) const noexcept {
    return in_topo_.edge_dest(edge_id);
  }

  size_t in_degree(Node N) const noexcept { return in_topo_.degree(N); }

  size_t in_degree(Node N, const EdgeTypeID& edge_type) const noexcept {
    return in_topo_.degree(N, edge_type);
  }

  Edge original_edge_id_using_in_edge(Edge in_edge) const noexcept {
    return in_topo_.original_edge_id(in_edge);
  }

  /// Returns an edge iterator to an edge with some node and key by
  /// searching for the key via the node's outgoing or incoming edges.
  /// If not found, returns nothing.
  edges_range FindAllEdgesSingleType(Node src, Node dst) const {
    // TODO: Similar to IsConnectedWithEdgeType, we should be able to switch
    // between searching out going topology or incoming topology. However, incoming
    // topology will return a different range of incoming edges instead of outgoing
    // edges. Can we convert easily between outing and incoming edge range
    if (OutTopoBase::degree(src) == 0 || in_topo_.degree(dst) == 0) {
      return MakeStandardRange<edge_iterator>(Edge{0}, Edge{0});
    }

    return OutTopoBase::FindAllEdgesSingleType(src, dst);
  }

  /// Check if vertex src is connected to vertex dst with the given edge edge_type
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @param edge_type edge_type of the edge
  /// @returns true iff the edge exists
  bool IsConnectedWithEdgeType(
      Node src, Node dst, const EdgeTypeID& edge_type) const {
    const auto d_out = OutTopoBase::degree(src, edge_type);
    const auto d_in = in_topo_.degree(dst, edge_type);
    if (d_out == 0 || d_in == 0) {
      return false;
    }

    if (d_out < d_in) {
      return OutTopoBase::IsConnectedWithEdgeType(src, dst, edge_type);
    } else {
      return in_topo_.IsConnectedWithEdgeType(dst, src, edge_type);
    }
  }

  /// Check if vertex src is connected to vertex dst with any edge edge_type
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @returns true iff the edge exists
  bool IsConnected(Node src, Node dst) const {
    const auto d_out = OutTopoBase::degree(src);
    const auto d_in = in_topo_.degree(dst);
    if (d_out == 0 || d_in == 0) {
      return false;
    }

    if (d_out < d_in) {
      return OutTopoBase::IsConnected(src, dst);
    } else {
      return in_topo_.IsConnected(dst, src);
    }
  }

private:
  // A bit tricky. Can't use edge_type_index after moving it to member variable
  // edge_type_index_. Order of initialization matters here
  explicit EdgeTypeAwareBiDirTopology(
      const PropertyGraph* pg,
      std::unique_ptr<EdgeTypeIndex>&& edge_type_index) noexcept
      : OutTopoBase{OutTopoBase::MakeFromDefaultTopology(
            pg, edge_type_index.get())},
        in_topo_{EdgeTypeAwareTopology::MakeFromTransposeTopology(
            pg, edge_type_index.get())},
        edge_type_index_(std::move(edge_type_index)) {}

  EdgeTypeAwareTopology in_topo_;
  std::unique_ptr<EdgeTypeIndex> edge_type_index_;
};

}  // namespace katana

#endif

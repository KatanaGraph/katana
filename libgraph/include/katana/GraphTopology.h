#ifndef KATANA_LIBGRAPH_KATANA_GRAPHTOPOLOGY_H_
#define KATANA_LIBGRAPH_KATANA_GRAPHTOPOLOGY_H_

#include <memory>
#include <utility>
#include <vector>

#include <boost/iterator/counting_iterator.hpp>

#include "arrow/util/bitmap.h"
#include "katana/CompileTimeIntrospection.h"
#include "katana/DynamicBitset.h"
#include "katana/Iterators.h"
#include "katana/Logging.h"
#include "katana/NUMAArray.h"
#include "katana/RDGTopology.h"
#include "katana/Result.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT PropertyGraph;

// TODO(amber): None of the topologies or views or PGViewCache can keep a member
// pointer to PropertyGraph because PropertyGraph can be moved. This issue plagues
// TypedPropertyGraph and TypedPropertyGraphView as well. If we really need to keep
// a pointer to parent PropertyGraph (which may be a good idea), we need to make
// PropertyGraph non-movable and non-copyable

/// Types used by all topologies
struct KATANA_EXPORT GraphTopologyTypes {
  using Node = uint32_t;
  using Edge = uint64_t;
  using PropertyIndex = uint64_t;
  using node_iterator = boost::counting_iterator<Node>;
  using edge_iterator = boost::counting_iterator<Edge>;
  using nodes_range = StandardRange<node_iterator>;
  using edges_range = StandardRange<edge_iterator>;
  using iterator = node_iterator;

  //TODO(emcginnis): Each of these *Vec types should really be *Array since they are not resizable
  using AdjIndexVec = NUMAArray<Edge>;
  using EdgeDestVec = NUMAArray<Node>;
  using PropIndexVec = NUMAArray<PropertyIndex>;
  using EntityTypeIDVec = NUMAArray<EntityTypeID>;
};

class KATANA_EXPORT EdgeShuffleTopology;
class KATANA_EXPORT EdgeTypeAwareTopology;

/********************/
/* Topology classes */
/********************/

/// A graph topology represents the adjacency information for a graph in CSR
/// format.
class KATANA_EXPORT GraphTopology : public GraphTopologyTypes {
public:
  GraphTopology() = default;
  GraphTopology(GraphTopology&&) = default;
  GraphTopology& operator=(GraphTopology&&) = default;

  GraphTopology(const GraphTopology&) = delete;
  GraphTopology& operator=(const GraphTopology&) = delete;
  virtual ~GraphTopology();

  GraphTopology(
      const Edge* adj_indices, size_t num_nodes, const Node* dests,
      size_t num_edges) noexcept;

  GraphTopology(
      NUMAArray<Edge>&& adj_indices, NUMAArray<Node>&& dests) noexcept;

  static GraphTopology Copy(const GraphTopology& that) noexcept;

  uint64_t NumNodes() const noexcept { return adj_indices_.size(); }

  uint64_t NumEdges() const noexcept { return dests_.size(); }

  const Edge* AdjData() const noexcept { return adj_indices_.data(); }

  const Node* DestData() const noexcept { return dests_.data(); }

  /// Checks equality against another instance of GraphTopology.
  /// WARNING: Expensive operation due to element-wise checks on large arrays
  /// @param that: GraphTopology instance to compare against
  /// @returns true if topology arrays are equal
  bool Equals(const GraphTopology& that) const noexcept {
    if (this == &that) {
      return true;
    }
    if (NumNodes() != that.NumNodes()) {
      return false;
    }
    if (NumEdges() != that.NumEdges()) {
      return false;
    }

    return adj_indices_ == that.adj_indices_ && dests_ == that.dests_;
  }

  /// Gets all out-edges
  edges_range OutEdges() const noexcept {
    return MakeStandardRange<edge_iterator>(Edge{0}, Edge{NumEdges()});
  }

  /// Gets out-edges of some node.
  ///
  /// \param node node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range OutEdges(Node node) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(node <= adj_indices_.size());
    edge_iterator e_beg{node > 0 ? adj_indices_[node - 1] : 0};
    edge_iterator e_end{adj_indices_[node]};

    return MakeStandardRange(e_beg, e_end);
  }

  Node OutEdgeDst(Edge edge_id) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(edge_id < dests_.size());
    return dests_[edge_id];
  }

  Node GetEdgeSrc(const Edge& eid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(eid < NumEdges());

    if (eid < adj_indices_[0]) {
      return Node{0};
    }

    auto it = std::upper_bound(adj_indices_.begin(), adj_indices_.end(), eid);
    KATANA_LOG_DEBUG_ASSERT(it != adj_indices_.end());
    KATANA_LOG_DEBUG_ASSERT(*it > eid);

    auto d = std::distance(adj_indices_.begin(), it);
    KATANA_LOG_DEBUG_ASSERT(static_cast<size_t>(d) < NumNodes());
    KATANA_LOG_DEBUG_ASSERT(d > 0);

    return static_cast<Node>(d);
  }

  /// @param node node to get degree for
  /// @returns Degree of node N
  size_t OutDegree(Node node) const noexcept { return OutEdges(node).size(); }

  nodes_range Nodes() const noexcept {
    return MakeStandardRange<node_iterator>(
        Node{0}, static_cast<Node>(NumNodes()));
  }

  // Standard container concepts

  node_iterator begin() const noexcept { return node_iterator(0); }

  node_iterator end() const noexcept { return node_iterator(NumNodes()); }

  size_t size() const noexcept { return NumNodes(); }

  bool empty() const noexcept { return NumNodes() == 0; }

  // TODO(yan): This will become GetEdgePropertyIndex(OutEdgeHandle) once we
  // introduce new opaque indexing types.
  PropertyIndex GetEdgePropertyIndexFromOutEdge(
      const Edge& eid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(eid < NumEdges());
    return edge_prop_indices_[eid];
  }

  PropertyIndex GetNodePropertyIndex(const Node& nid) const noexcept {
    return nid;
  }

  // TODO(amber): These two methods are a short term fix. The nature of
  // PropertyIndex is expected to change post grouping of properties.
  Node GetLocalNodeID(const Node& nid) const noexcept {
    return static_cast<Node>(GetNodePropertyIndex(nid));
  }

  Edge GetLocalEdgeIDFromOutEdge(const Edge& eid) const noexcept {
    return GetEdgePropertyIndexFromOutEdge(eid);
  }

  // We promote these methods to the base topology to make it easier to search among all
  // already cached topologies using the same predicate.
  bool is_transposed() const noexcept {
    return has_transpose_state(katana::RDGTopology::TransposeKind::kYes);
  }

  bool has_transpose_state(
      const katana::RDGTopology::TransposeKind& expected) const noexcept {
    return tpose_state_ == expected;
  }

  katana::RDGTopology::TransposeKind transpose_state() const noexcept {
    return tpose_state_;
  }

  katana::RDGTopology::EdgeSortKind edge_sort_state() const noexcept {
    return edge_sort_state_;
  }

  void Print() const noexcept;

protected:
  void SetTransposeState(
      katana::RDGTopology::TransposeKind tpose_state) noexcept {
    tpose_state_ = tpose_state;
  }

  void SetEdgeSortState(
      katana::RDGTopology::EdgeSortKind edge_sort_state) noexcept {
    edge_sort_state_ = edge_sort_state;
  }

  void SetEdgePropIndices(PropIndexVec&& edge_prop_indices) noexcept {
    KATANA_LOG_DEBUG_ASSERT(edge_prop_indices_.size() == NumEdges());
    edge_prop_indices_ = std::move(edge_prop_indices);
  }

  const PropertyIndex* edge_property_index_data() const noexcept {
    return edge_prop_indices_.data();
  }

private:
  // need these friend relationships to construct instances of friend classes below
  // by moving NUMAArrays in this class.
  friend class EdgeShuffleTopology;
  friend class EdgeTypeAwareTopology;

  NUMAArray<Edge>& GetAdjIndices() noexcept { return adj_indices_; }
  NUMAArray<Node>& GetDests() noexcept { return dests_; }

  NUMAArray<Edge> adj_indices_;
  NUMAArray<Node> dests_;

  katana::RDGTopology::TransposeKind tpose_state_{
      katana::RDGTopology::TransposeKind::kNo};
  katana::RDGTopology::EdgeSortKind edge_sort_state_{
      katana::RDGTopology::EdgeSortKind::kAny};

  // TODO(amber): In the future, we may need to keep a copy of edge_type_ids in
  // addition to edge_prop_indices_. Today, we assume that we can use
  // PropertyGraph.edge_type_set_id(edge_prop_indices_[edge_id]) to obtain
  // edge_type_id. This may not be true when we group properties
  // when this is done, the Write path must also be updated to pass the edge_type_ids index
  // to RDG. For now, we pass nullptr.
  PropIndexVec edge_prop_indices_;
};

// TODO(amber): In the future, when we group properties e.g., by node or edge type,
// this class might get merged with ShuffleTopology. Not doing it at the moment to
// avoid having to keep unnecessary arrays like node_property_indices_
class KATANA_EXPORT EdgeShuffleTopology : public GraphTopology {
  using Base = GraphTopology;

public:
  EdgeShuffleTopology() = default;
  EdgeShuffleTopology(EdgeShuffleTopology&&) = default;
  EdgeShuffleTopology& operator=(EdgeShuffleTopology&&) = default;

  EdgeShuffleTopology(const EdgeShuffleTopology&) = delete;
  EdgeShuffleTopology& operator=(const EdgeShuffleTopology&) = delete;
  virtual ~EdgeShuffleTopology();

  bool is_valid() const noexcept { return is_valid_; }

  void invalidate() noexcept { is_valid_ = false; }

  bool has_edges_sorted_by(
      const katana::RDGTopology::EdgeSortKind& kind) const noexcept {
    return (kind == katana::RDGTopology::EdgeSortKind::kAny) ||
           (kind == edge_sort_state());
  }

  using Base::GetLocalEdgeIDFromOutEdge;

  static std::shared_ptr<EdgeShuffleTopology> MakeTransposeCopy(
      const PropertyGraph* pg);
  static std::shared_ptr<EdgeShuffleTopology> MakeOriginalCopy(
      const PropertyGraph* pg);

  static std::shared_ptr<EdgeShuffleTopology> Make(
      PropertyGraph* pg, const katana::RDGTopology::TransposeKind& tpose_todo,
      const katana::RDGTopology::EdgeSortKind& edge_sort_todo) noexcept {
    std::shared_ptr<EdgeShuffleTopology> ret;

    if (tpose_todo == katana::RDGTopology::TransposeKind::kYes) {
      ret = MakeTransposeCopy(pg);
      KATANA_LOG_DEBUG_ASSERT(
          ret->has_transpose_state(katana::RDGTopology::TransposeKind::kYes));
    } else {
      ret = MakeOriginalCopy(pg);
      KATANA_LOG_DEBUG_ASSERT(
          ret->has_transpose_state(katana::RDGTopology::TransposeKind::kNo));
    }

    ret->sortEdges(pg, edge_sort_todo);
    return ret;
  }

  static std::shared_ptr<EdgeShuffleTopology> Make(
      katana::RDGTopology* rdg_topo);

  katana::Result<katana::RDGTopology> ToRDGTopology() const;

  edge_iterator FindEdge(const Node& src, const Node& dst) const noexcept;

  edges_range FindAllEdges(const Node& src, const Node& dst) const noexcept;

  bool HasEdge(const Node& src, const Node& dst) const noexcept {
    return FindEdge(src, dst) != OutEdges(src).end();
  }

protected:
  void SortEdgesByDestID() noexcept;

  void SortEdgesByTypeThenDest(const PropertyGraph* pg) noexcept;

  void SortEdgesByDestType(
      const PropertyGraph* pg, const PropIndexVec& node_prop_indices) noexcept;

  void sortEdges(
      const PropertyGraph* pg,
      const katana::RDGTopology::EdgeSortKind& edge_sort_todo) noexcept {
    switch (edge_sort_todo) {
    case katana::RDGTopology::EdgeSortKind::kAny:
      return;
    case katana::RDGTopology::EdgeSortKind::kSortedByDestID:
      SortEdgesByDestID();
      return;
    case katana::RDGTopology::EdgeSortKind::kSortedByEdgeType:
      SortEdgesByTypeThenDest(pg);
      return;
    case katana::RDGTopology::EdgeSortKind::kSortedByNodeType:
      KATANA_LOG_FATAL("Not implemented yet");
      return;
    default:
      KATANA_LOG_FATAL("switch-case fell through");
      return;
    }
  }

  EdgeShuffleTopology(
      const katana::RDGTopology::TransposeKind& tpose_todo,
      const katana::RDGTopology::EdgeSortKind& edge_sort_todo,
      AdjIndexVec&& adj_indices, EdgeDestVec&& dests,
      PropIndexVec&& edge_prop_indices) noexcept
      : Base(std::move(adj_indices), std::move(dests)), is_valid_(true) {
    SetTransposeState(tpose_todo);
    SetEdgeSortState(edge_sort_todo);
    SetEdgePropIndices(std::move(edge_prop_indices));
  }

private:
  bool is_valid_ = true;
};

/// This is a fully shuffled topology where both the nodes and edges can be sorted
class KATANA_EXPORT ShuffleTopology : public EdgeShuffleTopology {
  using Base = EdgeShuffleTopology;

public:
  ShuffleTopology() = default;
  ShuffleTopology(ShuffleTopology&&) = default;
  ShuffleTopology& operator=(ShuffleTopology&&) = default;

  ShuffleTopology(const ShuffleTopology&) = delete;
  ShuffleTopology& operator=(const ShuffleTopology&) = delete;

  virtual ~ShuffleTopology();

  PropertyIndex GetNodePropertyIndex(const Node& nid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(nid < NumNodes());
    return node_prop_indices_[nid];
  }

  bool has_nodes_sorted_by(
      const katana::RDGTopology::NodeSortKind& kind) const noexcept {
    if (kind == katana::RDGTopology::NodeSortKind::kAny) {
      return true;
    }
    return node_sort_state_ == kind;
  }

  katana::RDGTopology::NodeSortKind node_sort_state() const noexcept {
    return node_sort_state_;
  }

  static std::shared_ptr<ShuffleTopology> MakeFrom(
      const PropertyGraph* pg, const EdgeShuffleTopology& seed_topo) noexcept;

  static std::shared_ptr<ShuffleTopology> MakeSortedByDegree(
      const PropertyGraph* pg, const EdgeShuffleTopology& seed_topo) noexcept;

  static std::shared_ptr<ShuffleTopology> MakeSortedByNodeType(
      const PropertyGraph* pg, const EdgeShuffleTopology& seed_topo) noexcept;

  static std::shared_ptr<ShuffleTopology> MakeFromTopo(
      const PropertyGraph* pg, const EdgeShuffleTopology& seed_topo,
      const katana::RDGTopology::NodeSortKind& node_sort_todo,
      const katana::RDGTopology::EdgeSortKind& edge_sort_todo) noexcept {
    std::shared_ptr<ShuffleTopology> ret;

    switch (node_sort_todo) {
    case katana::RDGTopology::NodeSortKind::kAny:
      ret = MakeFrom(pg, seed_topo);
      break;
    case katana::RDGTopology::NodeSortKind::kSortedByDegree:
      ret = MakeSortedByDegree(pg, seed_topo);
      break;
    case katana::RDGTopology::NodeSortKind::kSortedByNodeType:
      ret = MakeSortedByNodeType(pg, seed_topo);
      break;
    default:
      KATANA_LOG_FATAL("switch case fell through");
    }

    ret->sortEdges(pg, edge_sort_todo);

    return ret;
  }

  static std::shared_ptr<ShuffleTopology> Make(katana::RDGTopology* rdg_topo);

  katana::Result<katana::RDGTopology> ToRDGTopology() const;

private:
  template <typename CmpFunc>
  static std::shared_ptr<ShuffleTopology> MakeNodeSortedTopo(
      const EdgeShuffleTopology& seed_topo, const CmpFunc& cmp,
      const katana::RDGTopology::NodeSortKind& node_sort_todo) {
    GraphTopology::PropIndexVec node_prop_indices;
    node_prop_indices.allocateInterleaved(seed_topo.NumNodes());

    katana::ParallelSTL::iota(
        node_prop_indices.begin(), node_prop_indices.end(),
        GraphTopologyTypes::PropertyIndex{0});

    katana::ParallelSTL::sort(
        node_prop_indices.begin(), node_prop_indices.end(),
        [&](const auto& i1, const auto& i2) { return cmp(i1, i2); });

    GraphTopology::AdjIndexVec degrees;
    degrees.allocateInterleaved(seed_topo.NumNodes());

    katana::NUMAArray<GraphTopologyTypes::Node> old_to_new_map;
    old_to_new_map.allocateInterleaved(seed_topo.NumNodes());
    // TODO(amber): given 32-bit node ids, put a check here that
    // node_prop_indices.size() < 2^32
    katana::do_all(
        katana::iterate(size_t{0}, node_prop_indices.size()),
        [&](auto i) {
          // node_prop_indices[i] gives old node id
          old_to_new_map[node_prop_indices[i]] = i;
          degrees[i] = seed_topo.OutDegree(node_prop_indices[i]);
        },
        katana::no_stats());

    KATANA_LOG_DEBUG_ASSERT(
        std::is_sorted(degrees.begin(), degrees.end(), std::greater<>()));

    katana::ParallelSTL::partial_sum(
        degrees.begin(), degrees.end(), degrees.begin());

    GraphTopologyTypes::EdgeDestVec new_dest_vec;
    new_dest_vec.allocateInterleaved(seed_topo.NumEdges());

    GraphTopologyTypes::PropIndexVec edge_prop_indices;
    edge_prop_indices.allocateInterleaved(seed_topo.NumEdges());

    katana::do_all(
        katana::iterate(seed_topo.Nodes()),
        [&](auto old_src_id) {
          auto new_srd_id = old_to_new_map[old_src_id];
          auto new_out_index = new_srd_id > 0 ? degrees[new_srd_id - 1] : 0;

          for (auto e : seed_topo.OutEdges(old_src_id)) {
            auto new_edge_dest = old_to_new_map[seed_topo.OutEdgeDst(e)];
            KATANA_LOG_DEBUG_ASSERT(new_edge_dest < seed_topo.NumNodes());

            auto new_edge_id = new_out_index;
            ++new_out_index;
            KATANA_LOG_DEBUG_ASSERT(new_out_index <= degrees[new_srd_id]);

            new_dest_vec[new_edge_id] = new_edge_dest;

            // copy over edge_property_index mapping from old edge to new edge
            edge_prop_indices[new_edge_id] =
                seed_topo.GetEdgePropertyIndexFromOutEdge(e);
          }
          KATANA_LOG_DEBUG_ASSERT(new_out_index == degrees[new_srd_id]);
        },
        katana::steal(), katana::no_stats());

    return std::make_shared<ShuffleTopology>(ShuffleTopology{
        seed_topo.transpose_state(), node_sort_todo,
        seed_topo.edge_sort_state(), std::move(degrees),
        std::move(node_prop_indices), std::move(new_dest_vec),
        std::move(edge_prop_indices)});
  }

  ShuffleTopology(
      const katana::RDGTopology::TransposeKind& tpose_todo,
      const katana::RDGTopology::NodeSortKind& node_sort_todo,
      const katana::RDGTopology::EdgeSortKind& edge_sort_todo,
      AdjIndexVec&& adj_indices, PropIndexVec&& node_prop_indices,
      EdgeDestVec&& dests, PropIndexVec&& edge_prop_indices) noexcept
      : Base(
            tpose_todo, edge_sort_todo, std::move(adj_indices),
            std::move(dests), std::move(edge_prop_indices)),
        node_sort_state_(node_sort_todo),
        node_prop_indices_(std::move(node_prop_indices)) {
    KATANA_LOG_DEBUG_ASSERT(node_prop_indices_.size() == NumNodes());
  }

  katana::RDGTopology::NodeSortKind node_sort_state_{
      katana::RDGTopology::NodeSortKind::kAny};

  // TODO(amber): In the future, we may need to keep a copy of node_type_ids in
  // addition to node_prop_indices_. Today, we assume that we can use
  // PropertyGraph.node_type_set_id(node_prop_indices_[node_id]) to obtain
  // node_type_id. This may not be true when we group properties
  PropIndexVec node_prop_indices_;
};

namespace internal {
// TODO(amber): make private
template <typename Topo>
struct EdgeDestComparator {
  const Topo* topo_;

  bool operator()(const typename Topo::Edge& e, const typename Topo::Node& n)
      const noexcept {
    return topo_->OutEdgeDst(e) < n;
  }

  bool operator()(const typename Topo::Node& n, const typename Topo::Edge& e)
      const noexcept {
    return n < topo_->OutEdgeDst(e);
  }
};
}  // end namespace internal

class KATANA_EXPORT CondensedTypeIDMap : public GraphTopologyTypes {
  /// map an integer id to each unique edge edge_type in the graph, such that, the
  /// integer ids assigned are contiguous, i.e., 0 .. num_unique_types-1
  using TypeIDToIndexMap = std::unordered_map<EntityTypeID, uint32_t>;
  /// reverse map that allows looking up edge_type using its integer index
  using IndexToTypeIDMap = std::vector<EntityTypeID>;

public:
  using EdgeTypeIDRange =
      katana::StandardRange<IndexToTypeIDMap::const_iterator>;

  CondensedTypeIDMap() = default;
  CondensedTypeIDMap(CondensedTypeIDMap&&) = default;
  CondensedTypeIDMap& operator=(CondensedTypeIDMap&&) = default;

  CondensedTypeIDMap(const CondensedTypeIDMap&) = delete;
  CondensedTypeIDMap& operator=(const CondensedTypeIDMap&) = delete;

  static std::shared_ptr<CondensedTypeIDMap> MakeFromEdgeTypes(
      const PropertyGraph* pg) noexcept;
  // TODO(amber): add MakeFromNodeTypes

  EntityTypeID GetType(uint32_t index) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(size_t(index) < index_to_type_map_.size());
    return index_to_type_map_[index];
  }

  uint32_t GetIndex(const EntityTypeID& edge_type) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(type_to_index_map_.count(edge_type) > 0);
    return type_to_index_map_.at(edge_type);
  }

  size_t num_unique_types() const noexcept { return index_to_type_map_.size(); }

  /// @param edge_type: edge_type to check
  /// @returns true iff there exists some edge in the graph with that edge_type
  bool has_edge_type_id(const EntityTypeID& edge_type) const noexcept {
    return (type_to_index_map_.find(edge_type) != type_to_index_map_.cend());
  }

  /// Wrapper to get the distinct edge types in the graph.
  ///
  /// @returns Range of the distinct edge types
  EdgeTypeIDRange distinct_edge_type_ids() const noexcept {
    return EdgeTypeIDRange{
        index_to_type_map_.cbegin(), index_to_type_map_.cend()};
  }

  bool is_valid() const noexcept { return is_valid_; }
  void invalidate() noexcept { is_valid_ = false; };

  const EntityTypeID* index_to_type_map_data() const noexcept {
    return &index_to_type_map_[0];
  }

  //TODO:(emcginnis) when ArrayView is available, we should use that here
  bool index_to_type_map_matches(size_t size, const EntityTypeID* other) const {
    if (size != num_unique_types()) {
      return false;
    }
    for (size_t i = 0; i < num_unique_types(); i++) {
      if (other[i] != GetType(i)) {
        return false;
      }
    }
    return true;
  }

private:
  CondensedTypeIDMap(
      TypeIDToIndexMap&& type_to_index,
      IndexToTypeIDMap&& index_to_type) noexcept
      : type_to_index_map_(std::move(type_to_index)),
        index_to_type_map_(std::move(index_to_type)),
        is_valid_(true) {
    KATANA_LOG_ASSERT(index_to_type_map_.size() == type_to_index_map_.size());
  }

  TypeIDToIndexMap type_to_index_map_;
  IndexToTypeIDMap index_to_type_map_;
  bool is_valid_ = true;
};

/// store adjacency indices per each node such that they are divided by edge edge_type type.
/// Requires sorting the graph by edge edge_type type
class KATANA_EXPORT EdgeTypeAwareTopology : public EdgeShuffleTopology {
  using Base = EdgeShuffleTopology;

public:
  EdgeTypeAwareTopology(EdgeTypeAwareTopology&&) = default;
  EdgeTypeAwareTopology& operator=(EdgeTypeAwareTopology&&) = default;

  EdgeTypeAwareTopology(const EdgeTypeAwareTopology&) = delete;
  EdgeTypeAwareTopology& operator=(const EdgeTypeAwareTopology&) = delete;

  virtual ~EdgeTypeAwareTopology();

  static std::shared_ptr<EdgeTypeAwareTopology> MakeFrom(
      const PropertyGraph* pg,
      std::shared_ptr<const CondensedTypeIDMap> edge_type_index,
      EdgeShuffleTopology&& e_topo) noexcept;

  static std::shared_ptr<EdgeTypeAwareTopology> Make(
      katana::RDGTopology* rdg_topo,
      std::shared_ptr<const CondensedTypeIDMap> edge_type_index,
      EdgeShuffleTopology&& e_topo);

  using Base::edge_sort_state;
  using Base::has_transpose_state;
  using Base::invalidate;
  using Base::is_transposed;
  using Base::is_valid;
  using Base::OutDegree;
  using Base::OutEdges;
  using Base::transpose_state;

  /// @param N node to get edges for
  /// @param edge_type edge_type to get edges of
  /// @returns Range to edges of node N that have edge type == edge_type
  edges_range OutEdges(Node N, const EntityTypeID& edge_type) const noexcept {
    // per_type_adj_indices_ is expanded so that it stores P prefix sums per node, where
    // P == edge_type_index_->num_unique_types()
    // We pick the prefix sum based on the index of the edge_type provided
    KATANA_LOG_DEBUG_ASSERT(edge_type_index_->num_unique_types() > 0);
    auto beg_idx = (N * edge_type_index_->num_unique_types()) +
                   edge_type_index_->GetIndex(edge_type);
    edge_iterator e_beg{
        (beg_idx == 0) ? 0 : per_type_adj_indices_[beg_idx - 1]};

    auto end_idx = (N * edge_type_index_->num_unique_types()) +
                   edge_type_index_->GetIndex(edge_type);
    KATANA_LOG_DEBUG_ASSERT(end_idx < per_type_adj_indices_.size());
    edge_iterator e_end{per_type_adj_indices_[end_idx]};

    return katana::MakeStandardRange(e_beg, e_end);
  }

  /// @param N node to get degree for
  /// @param edge_type edge_type to get degree of
  /// @returns Degree of node N
  size_t OutDegree(Node N, const EntityTypeID& edge_type) const noexcept {
    return OutEdges(N, edge_type).size();
  }

  auto GetDistinctEdgeTypes() const noexcept {
    return edge_type_index_->distinct_edge_type_ids();
  }

  bool DoesEdgeTypeExist(const EntityTypeID& edge_type) const noexcept {
    return edge_type_index_->has_edge_type_id(edge_type);
  }

  /// Returns all edges from src to dst with some edge_type.  If not found, returns
  /// empty range.
  edges_range FindAllEdges(
      Node node, Node key, const EntityTypeID& edge_type) const noexcept {
    auto e_range = OutEdges(node, edge_type);
    if (e_range.empty()) {
      return e_range;
    }

    internal::EdgeDestComparator<EdgeTypeAwareTopology> comp{this};
    auto [first_it, last_it] =
        std::equal_range(e_range.begin(), e_range.end(), key, comp);

    if (first_it == e_range.end() || OutEdgeDst(*first_it) != key) {
      // return empty range
      return MakeStandardRange(e_range.end(), e_range.end());
    }

    auto ret_range = MakeStandardRange(first_it, last_it);
    for ([[maybe_unused]] auto e : ret_range) {
      KATANA_LOG_DEBUG_ASSERT(OutEdgeDst(e) == key);
    }
    return ret_range;
  }

  /// Returns an edge iterator to an edge with some node and key by
  /// searching for the key via the node's outgoing or incoming edges.
  /// If not found, returns nothing.
  // TODO(amber): Assess the usefulness of this method. This method cannot return
  // edges of all types. Only the first found type. We should however support
  // FindAllEdges(src, dst) or FindEdge(src, dst) that doesn't care about edge type
  edges_range FindAllEdges(Node src, Node dst) const {
    // trivial check; can't be connected if degree is 0
    auto empty_range = MakeStandardRange<edge_iterator>(Edge{0}, Edge{0});
    if (OutDegree(src) == 0) {
      return empty_range;
    }

    // loop through all type_ids
    for (const EntityTypeID& edge_type : GetDistinctEdgeTypes()) {
      // always use out edges (we want an id to the out edge returned)
      edges_range r = FindAllEdges(src, dst, edge_type);

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
  bool HasEdge(Node src, Node dst, const EntityTypeID& edge_type) const {
    auto e_range = OutEdges(src, edge_type);
    if (e_range.empty()) {
      return false;
    }

    internal::EdgeDestComparator<EdgeTypeAwareTopology> comp{this};
    return std::binary_search(e_range.begin(), e_range.end(), dst, comp);
  }

  /// Search over all edges of each type between src and dst until an edge satisfying func is
  /// found.
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @returns true iff an edge satisfying func exists
  template <typename TestFunc>
  bool HasEdgeSatisfyingPredicate(
      Node src, Node dst, const TestFunc& func) const noexcept {
    // ensure that return type is bool
    static_assert(std::is_same_v<std::invoke_result_t<TestFunc, Edge>, bool>);

    for (const auto& edge_type : GetDistinctEdgeTypes()) {
      for (auto e : FindAllEdges(src, dst, edge_type)) {
        if (func(e)) {
          return true;
        }
      }
    }
    return false;
  }

  /// Search over all out edges of src of each type until an edge satisfying func is
  /// found.
  ///
  /// @param src source node of the edge
  /// @returns true iff an edge satisfying func exists
  template <typename TestFunc>
  bool HasOutEdgeSatisfyingPredicate(
      Node src, const TestFunc& func) const noexcept {
    // ensure that return type is bool
    static_assert(std::is_same_v<std::invoke_result_t<TestFunc, Edge>, bool>);

    for (const auto& edge_type : GetDistinctEdgeTypes()) {
      for (auto e : OutEdges(src, edge_type)) {
        if (func(e)) {
          return true;
        }
      }
    }
    return false;
  }

  /// Check if vertex src is connected to vertex dst with any edge edge_type
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @returns true iff the edge exists
  bool HasEdge(Node src, Node dst) const {
    // trivial check; can't be connected if degree is 0

    if (OutDegree(src) == 0ul) {
      return false;
    }

    for (const auto& edge_type : GetDistinctEdgeTypes()) {
      if (HasEdge(src, dst, edge_type)) {
        return true;
      }
    }
    return false;
  }

  katana::Result<katana::RDGTopology> ToRDGTopology() const;

private:
  // Must invoke SortAllEdgesByDataThenDst() before
  // calling this function
  static AdjIndexVec CreatePerEdgeTypeAdjacencyIndex(
      const PropertyGraph& pg, const CondensedTypeIDMap& edge_type_index,
      const EdgeShuffleTopology& e_topo) noexcept;

  EdgeTypeAwareTopology(
      EdgeShuffleTopology&& e_topo,
      std::shared_ptr<const CondensedTypeIDMap> edge_type_index,
      AdjIndexVec&& per_type_adj_indices) noexcept
      : Base(std::move(e_topo)),
        edge_type_index_(std::move(edge_type_index)),
        per_type_adj_indices_(std::move(per_type_adj_indices)) {
    KATANA_LOG_DEBUG_ASSERT(edge_type_index_);

    KATANA_LOG_DEBUG_ASSERT(
        per_type_adj_indices_.size() ==
        NumNodes() * edge_type_index_->num_unique_types());
  }

  std::shared_ptr<const CondensedTypeIDMap> edge_type_index_;
  AdjIndexVec per_type_adj_indices_;
};

/****************************/
/* Topology wrapper classes */
/****************************/

template <typename Topo>
class KATANA_EXPORT BasicTopologyWrapper : public GraphTopologyTypes {
public:
  explicit BasicTopologyWrapper(std::shared_ptr<const Topo> t) noexcept
      : topo_ptr_(std::move(t)) {
    KATANA_LOG_DEBUG_ASSERT(topo_ptr_);
  }

  auto NumNodes() const noexcept { return topo().NumNodes(); }

  auto NumEdges() const noexcept { return topo().NumEdges(); }

  /// Gets all out-edges.
  auto OutEdges() const noexcept { return topo().OutEdges(); }

  /// Gets the edge range of some node.
  ///
  /// \param node node to get the edge range of
  /// \returns iterable edge range for node.
  auto OutEdges(const Node& N) const noexcept { return topo().OutEdges(N); }

  auto OutEdgeDst(const Edge& eid) const noexcept {
    return topo().OutEdgeDst(eid);
  }

  auto GetEdgeSrc(const Edge& eid) const noexcept {
    return topo().GetEdgeSrc(eid);
  }

  /// @param node node to get degree for
  /// @returns Degree of node N
  auto OutDegree(const Node& node) const noexcept {
    return topo().OutDegree(node);
  }

  auto Nodes() const noexcept { return topo().Nodes(); }

  // Standard container concepts

  auto begin() const noexcept { return topo().begin(); }

  auto end() const noexcept { return topo().end(); }

  auto size() const noexcept { return topo().size(); }

  auto empty() const noexcept { return topo().empty(); }

  auto GetEdgePropertyIndexFromOutEdge(const Edge& e) const noexcept {
    return topo().GetEdgePropertyIndexFromOutEdge(e);
  }

  auto GetNodePropertyIndex(const Node& nid) const noexcept {
    return topo().GetNodePropertyIndex(nid);
  }
  auto GetLocalNodeID(const Node& nid) const noexcept {
    return topo().GetLocalNodeID(nid);
  }

  auto GetLocalEdgeIDFromOutEdge(const Edge& eid) const noexcept {
    return topo().GetLocalEdgeIDFromOutEdge(eid);
  }
  void Print() const noexcept { topo_ptr_->Print(); }

protected:
  const Topo& topo() const noexcept { return *topo_ptr_.get(); }

private:
  std::shared_ptr<const Topo> topo_ptr_;
};

template <typename OutTopo, typename InTopo>
class KATANA_EXPORT BasicBiDirTopoWrapper
    : public BasicTopologyWrapper<OutTopo> {
  using Base = BasicTopologyWrapper<OutTopo>;

public:
  BasicBiDirTopoWrapper(
      std::shared_ptr<const OutTopo> out_topo,
      std::shared_ptr<const InTopo> in_topo) noexcept
      : Base(std::move(out_topo)), in_topo_(std::move(in_topo)) {
    KATANA_LOG_DEBUG_ASSERT(in_topo_);

    KATANA_LOG_DEBUG_ASSERT(in_topo_->is_transposed());

    KATANA_LOG_DEBUG_ASSERT(out().NumNodes() == in_topo_->NumNodes());
    KATANA_LOG_DEBUG_ASSERT(out().NumEdges() == in_topo_->NumEdges());
  }

  auto InEdges(const GraphTopologyTypes::Node& node) const noexcept {
    return in().OutEdges(node);
  }

  auto InDegree(const GraphTopologyTypes::Node& node) const noexcept {
    return in().OutDegree(node);
  }

  auto InEdgeSrc(const GraphTopologyTypes::Edge& edge_id) const noexcept {
    return in().OutEdgeDst(edge_id);
  }

  // TODO(yan): This will become GetEdgePropertyIndex(InEdgeHandle) once we
  // introduce new opaque indexing types.
  auto GetEdgePropertyIndexFromInEdge(
      const GraphTopologyTypes::Edge& eid) const noexcept {
    return in().GetEdgePropertyIndexFromOutEdge(eid);
  }

  auto GetLocalEdgeIDFromInEdge(
      const GraphTopologyTypes::Edge& eid) const noexcept {
    return in().GetLocalEdgeIDFromOutEdge(eid);
  }

protected:
  const OutTopo& out() const noexcept { return Base::topo(); }
  const InTopo& in() const noexcept { return *in_topo_.get(); }

private:
  std::shared_ptr<const InTopo> in_topo_;
};

template <typename OutTopo, typename InTopo>
class KATANA_EXPORT UndirectedTopologyWrapper : public GraphTopologyTypes {
  // Important:
  // We assign fake Edge IDs to in_edges to separate them from out edges
  // fake in-edge-ID == real in-edge-ID + out().NumEdges();

public:
  using edge_iterator =
      katana::DisjointRangesIterator<boost::counting_iterator<Edge>>;
  using edges_range = StandardRange<edge_iterator>;

  UndirectedTopologyWrapper(
      std::shared_ptr<const OutTopo> out,
      std::shared_ptr<const InTopo> in) noexcept
      : out_topo_(std::move(out)), in_topo_(std::move(in)) {}

  auto NumNodes() const noexcept { return out().NumNodes(); }

  // TODO(amber): Should it be sum of in and out edges?
  auto NumEdges() const noexcept { return out().NumEdges(); }

  /// Gets the edge range of some node.
  ///
  /// \param node node to get the edge range of
  /// \returns iterable edge range for node.
  edges_range UndirectedEdges(Node node) const noexcept {
    return MakeDisjointEdgesRange(out().OutEdges(node), in().OutEdges(node));
  }

  Node UndirectedEdgeNeighbor(Edge eid) const noexcept {
    if (is_in_edge(eid)) {
      return in().OutEdgeDst(real_in_edge_id(eid));
    }
    return out().OutEdgeDst(eid);
  }

  nodes_range Nodes() const noexcept {
    return MakeStandardRange<node_iterator>(
        Node{0}, static_cast<Node>(NumNodes()));
  }

  auto OutEdges() const noexcept {
    // return MakeDisjointEdgesRange(out().all_edges(), in().all_edges());
    // Note: We return edges from  outgoing topology, which is all the edges.
    // Commented line above will returns 2x the Edges.
    return out().OutEdges();
  }
  // Standard container concepts

  node_iterator begin() const noexcept { return node_iterator(0); }

  node_iterator end() const noexcept { return node_iterator(NumNodes()); }

  size_t size() const noexcept { return NumNodes(); }

  bool empty() const noexcept { return NumNodes() == 0; }

  /// @param node node to get degree for
  /// @returns Degree of node N
  size_t UndirectedDegree(Node node) const noexcept {
    return UndirectedEdges(node).size();
  }

  // TODO(yan): This will become GetEdgePropertyIndex(UndirectedEdgeHandle) once we
  // introduce new opaque indexing types.
  PropertyIndex GetEdgePropertyIndexFromUndirectedEdge(
      const Edge& eid) const noexcept {
    if (is_in_edge(eid)) {
      return in().GetEdgePropertyIndexFromOutEdge(real_in_edge_id(eid));
    }
    return out().GetEdgePropertyIndexFromOutEdge(eid);
  }

  PropertyIndex GetNodePropertyIndex(const Node& nid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(
        out().GetNodePropertyIndex(nid) == in().GetNodePropertyIndex(nid));
    return out().GetNodePropertyIndex(nid);
  }

  // TODO(amber): These two methods are a short term fix. The nature of
  // PropertyIndex is expected to change post grouping of properties.
  Node GetLocalNodeID(const Node& nid) const noexcept {
    return static_cast<Node>(GetNodePropertyIndex(nid));
  }

  Edge GetLocalEdgeIDFromUndirectedEdge(const Edge& eid) const noexcept {
    return GetEdgePropertyIndexFromUndirectedEdge(eid);
  }

protected:
  const OutTopo& out() const noexcept { return *out_topo_; }
  const InTopo& in() const noexcept { return *in_topo_; }

private:
  bool is_in_edge(const Edge& eid) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(out().NumEdges() > 0);
    return eid >= fake_id_offset();
  }

  Edge fake_id_offset() const noexcept {
    return out().NumEdges() +
           1;  // +1 so that last edge iterator of out() is different from first edge of in()
  }

  Edge real_in_edge_id(const Edge& id) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(id >= out().NumEdges());
    return id - fake_id_offset();
  }

  template <typename I>
  static std::pair<I, I> RangeToPair(const StandardRange<I>& r) noexcept {
    return std::make_pair(r.begin(), r.end());
  }

  template <typename R>
  edges_range MakeDisjointEdgesRange(
      const R& out_range, const R& in_range) const noexcept {
    auto out_iter_p = RangeToPair(out_range);
    auto in_iter_p = RangeToPair(in_range);

    in_iter_p.first += fake_id_offset();
    in_iter_p.second += fake_id_offset();

    edge_iterator b = MakeDisjointRangesBegin(out_iter_p, in_iter_p);
    edge_iterator e = MakeDisjointRangesEnd(out_iter_p, in_iter_p);

    return MakeStandardRange(b, e);
  }

  std::shared_ptr<const OutTopo> out_topo_;
  std::shared_ptr<const InTopo> in_topo_;
};

template <typename Topo>
class SortedTopologyWrapper : public BasicTopologyWrapper<Topo> {
  using Base = BasicTopologyWrapper<Topo>;

public:
  using typename Base::Node;

  explicit SortedTopologyWrapper(std::shared_ptr<const Topo> t) noexcept
      : Base(std::move(t)) {
    KATANA_LOG_DEBUG_ASSERT(Base::topo().has_edges_sorted_by(
        katana::RDGTopology::EdgeSortKind::kSortedByDestID));
  }

  auto FindEdge(const Node& src, const Node& dst) const noexcept {
    return Base::topo().FindEdge(src, dst);
  }

  auto HasEdge(const Node& src, const Node& dst) const noexcept {
    return Base::topo().HasEdge(src, dst);
  }

  auto FindAllEdges(const Node& src, const Node& dst) const noexcept {
    return Base::topo().FindAllEdges(src, dst);
  }
};

class KATANA_EXPORT EdgeTypeAwareBiDirTopology
    : public BasicBiDirTopoWrapper<
          EdgeTypeAwareTopology, EdgeTypeAwareTopology> {
  using Base =
      BasicBiDirTopoWrapper<EdgeTypeAwareTopology, EdgeTypeAwareTopology>;

public:
  explicit EdgeTypeAwareBiDirTopology(
      std::shared_ptr<const EdgeTypeAwareTopology> out_topo,
      std::shared_ptr<const EdgeTypeAwareTopology> in_topo) noexcept
      : Base(std::move(out_topo), std::move(in_topo)) {}

  auto GetDistinctEdgeTypes() const noexcept {
    return Base::out().GetDistinctEdgeTypes();
  }

  bool DoesEdgeTypeExist(const EntityTypeID& edge_type) const noexcept {
    return Base::out().DoesEdgeTypeExist(edge_type);
  }

  using Base::OutEdges;

  auto OutEdges(Node N, const EntityTypeID& edge_type) const noexcept {
    return Base::out().OutEdges(N, edge_type);
  }

  auto OutEdges(Node N) const noexcept { return Base::out().OutEdges(N); }

  auto InEdges(Node N, const EntityTypeID& edge_type) const noexcept {
    return Base::in().OutEdges(N, edge_type);
  }

  auto InEdges(Node N) const noexcept { return Base::in().OutEdges(N); }

  auto OutDegree(Node N, const EntityTypeID& edge_type) const noexcept {
    return Base::out().OutDegree(N, edge_type);
  }

  auto OutDegree(Node N) const noexcept { return Base::out().OutDegree(N); }

  auto InDegree(Node N, const EntityTypeID& edge_type) const noexcept {
    return Base::in().OutDegree(N, edge_type);
  }

  auto InDegree(Node N) const noexcept { return Base::in().OutDegree(N); }

  auto FindAllEdges(
      const Node& src, const Node& dst,
      const EntityTypeID& edge_type) const noexcept {
    return Base::out().FindAllEdges(src, dst, edge_type);
  }

  /// Returns an edge iterator to an edge with some node and key by
  /// searching for the key via the node's outgoing or incoming edges.
  /// If not found, returns nothing.
  edges_range FindAllEdges(Node src, Node dst) const {
    // TODO(amber): Similar to IsConnectedWithEdgeType, we should be able to switch
    // between searching outgoing topology or incoming topology. However, incoming
    // topology will return a different range of incoming edges instead of outgoing
    // edges. Can we convert easily between outing and incoming edge range
    if (Base::out().OutDegree(src) == 0 || Base::in().OutDegree(dst) == 0) {
      return MakeStandardRange<edge_iterator>(Edge{0}, Edge{0});
    }

    return Base::out().FindAllEdges(src, dst);
  }

  /// Check if vertex src is connected to vertex dst with the given edge edge_type
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @param edge_type edge_type of the edge
  /// @returns true iff the edge exists
  bool HasEdge(Node src, Node dst, const EntityTypeID& edge_type) const {
    const auto d_out = OutDegree(src, edge_type);
    const auto d_in = InDegree(dst, edge_type);
    if (d_out == 0 || d_in == 0) {
      return false;
    }

    if (d_out < d_in) {
      return Base::out().HasEdge(src, dst, edge_type);
    } else {
      return Base::in().HasEdge(dst, src, edge_type);
    }
  }

  /// Search over all edges of each type between src and dst until an edge satisfying func is
  /// found.
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @returns true iff the edge exists
  template <typename TestFunc>
  bool HasEdgeSatisfyingPredicate(
      Node src, Node dst, const TestFunc& func) const {
    const auto d_out = OutDegree(src);
    const auto d_in = InDegree(dst);
    if (d_out == 0 || d_in == 0) {
      return false;
    }

    // TODO(john) Figure out why queries were yielding incorrect results when
    // we add a branch here for d_out < d_in.
    return Base::out().HasEdgeSatisfyingPredicate(src, dst, func);
  }

  /// Search over all out edges of src of each type until an edge satisfying func is
  /// found.
  ///
  /// @param src source node of the edge
  /// @returns true iff the edge exists
  template <typename TestFunc>
  bool HasOutEdgeSatisfyingPredicate(Node src, const TestFunc& func) const {
    return Base::out().HasOutEdgeSatisfyingPredicate(src, func);
  }

  /// Search over all in edges of dst of each type until an edge satisfying func is
  /// found.
  ///
  /// @param dst destination node of the edge
  /// @returns true iff an edge satisfying func exists
  template <typename TestFunc>
  bool HasInEdgeSatisfyingPredicate(
      Node dst, const TestFunc& func) const noexcept {
    // TODO(john) Update this to use std::is_invocable_v.
    using RetTy = decltype(func(Edge{}));
    static_assert(
        std::is_same_v<RetTy, bool>);  // ensure that return type is bool.

    for (const auto& edge_type : GetDistinctEdgeTypes()) {
      for (auto e : InEdges(dst, edge_type)) {
        if (func(e)) {
          return true;
        }
      }
    }
    return false;
  }

  /// Check if vertex src is connected to vertex dst with any edge edge_type
  ///
  /// @param src source node of the edge
  /// @param dst destination node of the edge
  /// @returns true iff the edge exists
  bool HasEdge(Node src, Node dst) const {
    const auto d_out = OutDegree(src);
    const auto d_in = InDegree(dst);
    if (d_out == 0 || d_in == 0) {
      return false;
    }

    if (d_out < d_in) {
      return Base::out().HasEdge(src, dst);
    } else {
      return Base::in().HasEdge(dst, src);
    }
  }
};

template <typename Graph>
using has_undirected_t =
    decltype(std::declval<Graph>().UndirectedEdges(typename Graph::Node()));

template <typename Graph>
auto
Edges(const Graph& graph, typename Graph::Node node) {
  if constexpr (katana::is_detected_v<has_undirected_t, Graph>) {
    return graph.UndirectedEdges(node);
  } else {
    return graph.OutEdges(node);
  }
}

template <typename Graph>
auto
EdgeDst(const Graph& graph, typename Graph::Edge edge) {
  if constexpr (katana::is_detected_v<has_undirected_t, Graph>) {
    return graph.UndirectedEdgeNeighbor(edge);
  } else {
    return graph.OutEdgeDst(edge);
  }
}

template <typename Graph>
auto
Degree(const Graph& graph, typename Graph::Node node) {
  if constexpr (katana::is_detected_v<has_undirected_t, Graph>) {
    return graph.UndirectedDegree(node);
  } else {
    return graph.OutDegree(node);
  }
}

template <typename Topo>
class BasicPropGraphViewWrapper : public Topo {
  using Base = Topo;

public:
  explicit BasicPropGraphViewWrapper(
      PropertyGraph* pg, const Topo& topo) noexcept
      : Base(topo), prop_graph_(pg) {}

  const PropertyGraph* property_graph() const noexcept { return prop_graph_; }

private:
  PropertyGraph* prop_graph_;
};

namespace internal {
template <typename PGView>
struct PGViewBuilder {};

// Default view

using DefaultPGTopology = BasicTopologyWrapper<GraphTopology>;
using PGViewDefault = BasicPropGraphViewWrapper<DefaultPGTopology>;

template <>
struct PGViewBuilder<PGViewDefault> {
  template <typename ViewCache>
  static PGViewDefault BuildView(
      PropertyGraph* pg, ViewCache& viewCache) noexcept {
    auto topo = viewCache.GetDefaultTopology();
    return PGViewDefault{pg, DefaultPGTopology{topo}};
  }
};

// Transposed view

using TransposedTopology = BasicTopologyWrapper<EdgeShuffleTopology>;
using PGViewTransposed = BasicPropGraphViewWrapper<TransposedTopology>;

template <>
struct PGViewBuilder<PGViewTransposed> {
  template <typename ViewCache>
  static PGViewTransposed BuildView(
      PropertyGraph* pg, ViewCache& viewCache) noexcept {
    auto transposed_topo = viewCache.BuildOrGetEdgeShuffTopo(
        pg, katana::RDGTopology::TransposeKind::kYes,
        katana::RDGTopology::EdgeSortKind::kAny);

    return PGViewTransposed{pg, TransposedTopology(transposed_topo)};
  }
};

// Edges sorted by destination view

using EdgesSortedByDestTopology = SortedTopologyWrapper<EdgeShuffleTopology>;
using PGViewEdgesSortedByDestID =
    BasicPropGraphViewWrapper<EdgesSortedByDestTopology>;

template <>
struct PGViewBuilder<PGViewEdgesSortedByDestID> {
  template <typename ViewCache>
  static PGViewEdgesSortedByDestID BuildView(
      PropertyGraph* pg, ViewCache& viewCache) noexcept {
    auto sorted_topo = viewCache.BuildOrGetEdgeShuffTopo(
        pg, katana::RDGTopology::TransposeKind::kNo,
        katana::RDGTopology::EdgeSortKind::kSortedByDestID);

    viewCache.ReseatDefaultTopo(sorted_topo);

    return PGViewEdgesSortedByDestID{
        pg, EdgesSortedByDestTopology{sorted_topo}};
  }
};

// Nodes sorted by degree, edges sorted by destination view

using NodesSortedByDegreeEdgesSortedByDestIDTopology =
    SortedTopologyWrapper<ShuffleTopology>;
using PGViewNodesSortedByDegreeEdgesSortedByDestID =
    BasicPropGraphViewWrapper<NodesSortedByDegreeEdgesSortedByDestIDTopology>;

template <>
struct PGViewBuilder<PGViewNodesSortedByDegreeEdgesSortedByDestID> {
  template <typename ViewCache>
  static PGViewNodesSortedByDegreeEdgesSortedByDestID BuildView(
      PropertyGraph* pg, ViewCache& viewCache) noexcept {
    auto sorted_topo = viewCache.BuildOrGetShuffTopo(
        pg, katana::RDGTopology::TransposeKind::kNo,
        katana::RDGTopology::NodeSortKind::kSortedByDegree,
        katana::RDGTopology::EdgeSortKind::kSortedByDestID);

    return PGViewNodesSortedByDegreeEdgesSortedByDestID{
        pg, NodesSortedByDegreeEdgesSortedByDestIDTopology{sorted_topo}};
  }
};

// Bidirectional view

using SimpleBiDirTopology =
    BasicBiDirTopoWrapper<GraphTopology, EdgeShuffleTopology>;
using PGViewBiDirectional = BasicPropGraphViewWrapper<SimpleBiDirTopology>;

template <>
struct PGViewBuilder<PGViewBiDirectional> {
  template <typename ViewCache>
  static PGViewBiDirectional BuildView(
      PropertyGraph* pg, ViewCache& viewCache) noexcept {
    auto tpose_topo = viewCache.BuildOrGetEdgeShuffTopo(
        pg, katana::RDGTopology::TransposeKind::kYes,
        katana::RDGTopology::EdgeSortKind::kAny);
    auto bidir_topo =
        SimpleBiDirTopology{viewCache.GetDefaultTopology(), tpose_topo};

    return PGViewBiDirectional{pg, bidir_topo};
  }
};

// Undirected view

using UndirectedTopology =
    UndirectedTopologyWrapper<GraphTopology, EdgeShuffleTopology>;
using PGViewUnDirected = BasicPropGraphViewWrapper<UndirectedTopology>;

template <>
struct PGViewBuilder<PGViewUnDirected> {
  template <typename ViewCache>
  static internal::PGViewUnDirected BuildView(
      PropertyGraph* pg, ViewCache& viewCache) noexcept {
    auto tpose_topo = viewCache.BuildOrGetEdgeShuffTopo(
        pg, katana::RDGTopology::TransposeKind::kYes,
        katana::RDGTopology::EdgeSortKind::kAny);
    auto undir_topo =
        UndirectedTopology{viewCache.GetDefaultTopology(), tpose_topo};

    return PGViewUnDirected{pg, undir_topo};
  }
};

// Edge type aware bidirectional view

using PGViewEdgeTypeAwareBiDir =
    BasicPropGraphViewWrapper<EdgeTypeAwareBiDirTopology>;

template <>
struct PGViewBuilder<PGViewEdgeTypeAwareBiDir> {
  template <typename ViewCache>
  static PGViewEdgeTypeAwareBiDir BuildView(
      PropertyGraph* pg, ViewCache& viewCache) noexcept {
    auto out_topo = viewCache.BuildOrGetEdgeTypeAwareTopo(
        pg, katana::RDGTopology::TransposeKind::kNo);
    auto in_topo = viewCache.BuildOrGetEdgeTypeAwareTopo(
        pg, katana::RDGTopology::TransposeKind::kYes);

    // The new topology can replace the defaut one.
    viewCache.ReseatDefaultTopo(out_topo);

    return PGViewEdgeTypeAwareBiDir{
        pg, EdgeTypeAwareBiDirTopology{out_topo, in_topo}};
  }
};

}  // end namespace internal

struct PropertyGraphViews {
  using Default = internal::PGViewDefault;
  using Transposed = internal::PGViewTransposed;
  using BiDirectional = internal::PGViewBiDirectional;
  using Undirected = internal::PGViewUnDirected;
  using EdgesSortedByDestID = internal::PGViewEdgesSortedByDestID;
  using EdgeTypeAwareBiDir = internal::PGViewEdgeTypeAwareBiDir;
  using NodesSortedByDegreeEdgesSortedByDestID =
      internal::PGViewNodesSortedByDegreeEdgesSortedByDestID;
};

class KATANA_EXPORT PGViewCache {
  std::shared_ptr<GraphTopology> original_topo_{
      std::make_shared<GraphTopology>()};

  std::vector<std::shared_ptr<EdgeShuffleTopology>> edge_shuff_topos_;
  std::vector<std::shared_ptr<ShuffleTopology>> fully_shuff_topos_;
  std::vector<std::shared_ptr<EdgeTypeAwareTopology>> edge_type_aware_topos_;
  std::shared_ptr<CondensedTypeIDMap> edge_type_id_map_;
  // TODO(amber): define a node_type_id_map_;

  template <typename>
  friend struct internal::PGViewBuilder;

public:
  PGViewCache() = default;
  PGViewCache(GraphTopology&& original_topo)
      : original_topo_(
            std::make_shared<GraphTopology>(std::move(original_topo))) {}
  PGViewCache(PGViewCache&&) = default;
  PGViewCache& operator=(PGViewCache&&) = default;

  PGViewCache(const PGViewCache&) = delete;
  PGViewCache& operator=(const PGViewCache&) = delete;

  template <typename PGView>
  PGView BuildView(PropertyGraph* pg) noexcept {
    return internal::PGViewBuilder<PGView>::BuildView(pg, *this);
  }

  katana::Result<std::vector<katana::RDGTopology>> ToRDGTopology();

  template <typename PGView>
  PGView BuildView(
      const PropertyGraph* pg, const std::vector<std::string>& node_types,
      const std::vector<std::string>& edge_types) noexcept {
    return internal::PGViewBuilder<PGView>::BuildView(
        pg, node_types, edge_types, *this);
  }

  // Avoids a copy of the default topology.
  const GraphTopology& GetDefaultTopologyRef() const noexcept;

  // Purge cache and construct an empty topology as the default one.
  void DropAllTopologies() noexcept;

private:
  std::shared_ptr<GraphTopology> GetDefaultTopology() const noexcept;

  // Reseat the default topology pointer to a more constrained one.
  bool ReseatDefaultTopo(const std::shared_ptr<GraphTopology>& other) noexcept;

  std::shared_ptr<CondensedTypeIDMap> BuildOrGetEdgeTypeIndex(
      const PropertyGraph* pg) noexcept;

  // The pop flag ensures that the returned topology is not
  // in the edge_shuff_topos_ cache.
  std::shared_ptr<EdgeShuffleTopology> BuildOrGetEdgeShuffTopoImpl(
      PropertyGraph* pg, const katana::RDGTopology::TransposeKind& tpose_kind,
      const katana::RDGTopology::EdgeSortKind& sort_kind, bool pop) noexcept;

  std::shared_ptr<EdgeShuffleTopology> BuildOrGetEdgeShuffTopo(
      PropertyGraph* pg, const katana::RDGTopology::TransposeKind& tpose_kind,
      const katana::RDGTopology::EdgeSortKind& sort_kind) noexcept;

  std::shared_ptr<EdgeShuffleTopology> PopEdgeShuffTopo(
      PropertyGraph* pg, const katana::RDGTopology::TransposeKind& tpose_kind,
      const katana::RDGTopology::EdgeSortKind& sort_kind) noexcept;

  std::shared_ptr<ShuffleTopology> BuildOrGetShuffTopo(
      PropertyGraph* pg, const katana::RDGTopology::TransposeKind& tpose_kind,
      const katana::RDGTopology::NodeSortKind& node_sort_todo,
      const katana::RDGTopology::EdgeSortKind& edge_sort_todo) noexcept;

  std::shared_ptr<EdgeTypeAwareTopology> BuildOrGetEdgeTypeAwareTopo(
      PropertyGraph* pg,
      const katana::RDGTopology::TransposeKind& tpose_kind) noexcept;
};

/// Creates a uniform-random CSR GraphTopology instance, where each node as
///'edges_per_node' neighbors, chosen randomly
/// \p num_nodes number of nodes
/// \p edges_per_node number of out-going edges of each node
/// \r GraphTopology instance
KATANA_EXPORT GraphTopology CreateUniformRandomTopology(
    const size_t num_nodes, const size_t edges_per_node) noexcept;

/// A simple incremental topology builder for small sized graphs.
/// Typical usage:
/// AddNodes(10); // creates 10 nodes (0..9) with no edges
/// AddEdge(0, 3); // creates an edge between nodes 0 and 3.
/// Once done adding edges, call ConvertToCSR() to obtain a GraphTopology instance
template <bool IS_SYMMETRIC = false, bool ALLOW_MULTI_EDGE = false>
class KATANA_EXPORT TopologyBuilderImpl : public GraphTopologyTypes {
  using AdjVec = std::vector<Node>;

public:
  void AddNodes(size_t num) noexcept {
    all_nodes_adj_.resize(all_nodes_adj_.size() + num);
  }

  void AddEdge(Node src, Node dst) noexcept {
    AddEdgeImpl(src, dst);
    if constexpr (IS_SYMMETRIC) {
      AddEdgeImpl(dst, src);
    }
  }

  size_t degree(Node src) const noexcept {
    KATANA_LOG_DEBUG_ASSERT(IsValidNode(src));
    return all_nodes_adj_[src].size();
  }

  size_t num_nodes() const noexcept { return all_nodes_adj_.size(); }

  bool empty() const noexcept { return num_nodes() == size_t{0}; }

  size_t num_edges() const noexcept {
    size_t res = 0;
    for (const AdjVec& v : all_nodes_adj_) {
      res += v.size();
    }
    return res;
  }

  void Print() const noexcept {
    for (size_t n = 0; n < all_nodes_adj_.size(); ++n) {
      fmt::print("Node {}: [{}]\n", n, fmt::join(all_nodes_adj_[n], ", "));
    }
  }

  GraphTopology ConvertToCSR() const noexcept {
    NUMAArray<Edge> adj_indices;
    NUMAArray<Node> dests;

    adj_indices.allocateInterleaved(num_nodes());
    dests.allocateInterleaved(num_edges());

    size_t prefix_sum = 0;
    for (Node n = 0; n < num_nodes(); ++n) {
      adj_indices[n] = prefix_sum + degree(n);
      std::copy(
          all_nodes_adj_[n].begin(), all_nodes_adj_[n].end(),
          &dests[prefix_sum]);
      prefix_sum += degree(n);
    }

    return GraphTopology{std::move(adj_indices), std::move(dests)};
  }

private:
  bool IsValidNode(Node id) const noexcept {
    return id < all_nodes_adj_.size();
  }

  void AddEdgeImpl(Node src, Node dst) noexcept {
    KATANA_LOG_DEBUG_ASSERT(IsValidNode(src));
    auto& adj_list = all_nodes_adj_[src];

    if constexpr (ALLOW_MULTI_EDGE) {
      adj_list.emplace_back(dst);
    } else {
      auto it = std::find(adj_list.begin(), adj_list.end(), dst);
      bool not_found = (it == adj_list.end());
      KATANA_LOG_DEBUG_ASSERT(not_found);
      if (not_found) {
        adj_list.emplace_back(dst);
      }
    }
  }

  std::vector<AdjVec> all_nodes_adj_;
};

using AsymmetricGraphTopologyBuilder = TopologyBuilderImpl<false>;
using SymmetricGraphTopologyBuilder = TopologyBuilderImpl<true>;

}  // end namespace katana

#endif  // KATANA_LIBGRAPH_KATANA_GRAPHTOPOLOGY_H_

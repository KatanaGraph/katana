#include "katana/GraphTopology.h"

#include <math.h>

#include <iostream>

#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/RDGTopology.h"
#include "katana/Random.h"
#include "katana/Result.h"

katana::GraphTopology::~GraphTopology() = default;

void
katana::GraphTopology::Print() const noexcept {
  auto print_array = [](const auto& arr, const auto& name) {
    std::cout << name << ": [ ";
    for (const auto& i : arr) {
      std::cout << i << ", ";
    }
    std::cout << "]" << std::endl;
  };

  print_array(adj_indices_, "adj_indices_");
  print_array(dests_, "dests_");
}

katana::GraphTopology::GraphTopology(
    const Edge* adj_indices, size_t num_nodes, const Node* dests,
    size_t num_edges) noexcept {
  adj_indices_.allocateInterleaved(num_nodes);
  dests_.allocateInterleaved(num_edges);

  katana::ParallelSTL::copy(
      &adj_indices[0], &adj_indices[num_nodes], adj_indices_.begin());
  katana::ParallelSTL::copy(&dests[0], &dests[num_edges], dests_.begin());

  edge_prop_indices_.allocateInterleaved(num_edges);

  katana::ParallelSTL::iota(
      edge_prop_indices_.begin(), edge_prop_indices_.end(), Edge{0});
}

katana::GraphTopology::GraphTopology(
    NUMAArray<Edge>&& adj_indices, NUMAArray<Node>&& dests) noexcept
    : adj_indices_(std::move(adj_indices)), dests_(std::move(dests)) {
  edge_prop_indices_.allocateInterleaved(dests_.size());

  katana::ParallelSTL::iota(
      edge_prop_indices_.begin(), edge_prop_indices_.end(), Edge{0});
}

katana::GraphTopology
katana::GraphTopology::Copy(const GraphTopology& that) noexcept {
  return katana::GraphTopology(
      that.adj_indices_.data(), that.adj_indices_.size(), that.dests_.data(),
      that.dests_.size());
}

katana::ShuffleTopology::~ShuffleTopology() = default;

std::shared_ptr<katana::ShuffleTopology>
katana::ShuffleTopology::MakeFrom(
    const PropertyGraph*, const katana::EdgeShuffleTopology&) noexcept {
  KATANA_LOG_FATAL("Not implemented yet");
  std::shared_ptr<ShuffleTopology> ret;
  return ret;
}

katana::EdgeShuffleTopology::~EdgeShuffleTopology() = default;

std::shared_ptr<katana::EdgeShuffleTopology>
katana::EdgeShuffleTopology::MakeTransposeCopy(
    const katana::PropertyGraph* pg) {
  KATANA_LOG_DEBUG_ASSERT(pg);

  const auto& topology = pg->topology();
  if (topology.empty()) {
    EdgeShuffleTopology et;
    et.tpose_state_ = katana::RDGTopology::TransposeKind::kYes;
    return std::make_shared<EdgeShuffleTopology>(std::move(et));
  }

  GraphTopologyTypes::AdjIndexVec out_indices;
  GraphTopologyTypes::EdgeDestVec out_dests;
  GraphTopologyTypes::PropIndexVec edge_prop_indices;
  GraphTopologyTypes::AdjIndexVec out_dests_offset;

  out_indices.allocateInterleaved(topology.NumNodes());
  out_dests.allocateInterleaved(topology.NumEdges());
  edge_prop_indices.allocateInterleaved(topology.NumEdges());
  out_dests_offset.allocateInterleaved(topology.NumNodes());

  katana::ParallelSTL::fill(out_indices.begin(), out_indices.end(), Edge{0});

  // Keep a copy of old destinaton ids and compute number of
  // in-coming edges for the new prefix sum of out_indices.
  katana::do_all(
      katana::iterate(topology.OutEdges()),
      [&](Edge e) {
        // Counting outgoing edges in the tranpose graph by
        // counting incoming edges in the original graph
        auto dest = topology.OutEdgeDst(e);
        __sync_add_and_fetch(&(out_indices[dest]), 1);
      },
      katana::no_stats());

  // Prefix sum calculation of the edge index array
  katana::ParallelSTL::partial_sum(
      out_indices.begin(), out_indices.end(), out_indices.begin());

  // temporary buffer for storing the starting point of each node's transpose
  // adjacency
  out_dests_offset[0] = 0;
  katana::do_all(
      katana::iterate(Edge{1}, Edge{topology.NumNodes()}),
      [&](Edge n) { out_dests_offset[n] = out_indices[n - 1]; },
      katana::no_stats());

  // Update out_dests with the new destination ids
  // of the transposed graphs
  katana::do_all(
      katana::iterate(topology.Nodes()),
      [&](auto src) {
        // get all outgoing edges of a particular
        // node and reverse the edges.
        for (GraphTopology::Edge e : topology.OutEdges(src)) {
          // e = start index into edge array for a particular node
          // Destination node
          auto dest = topology.OutEdgeDst(e);
          // Location to save edge
          auto e_new = __sync_fetch_and_add(&(out_dests_offset[dest]), 1);
          // Save src as destination
          out_dests[e_new] = src;
          // remember the original edge ID to look up properties
          edge_prop_indices[e_new] = e;
        }
      },
      katana::steal(), katana::no_stats());

  return std::make_shared<EdgeShuffleTopology>(EdgeShuffleTopology{
      katana::RDGTopology::TransposeKind::kYes,
      katana::RDGTopology::EdgeSortKind::kAny, std::move(out_indices),
      std::move(out_dests), std::move(edge_prop_indices)});
}

std::shared_ptr<katana::EdgeShuffleTopology>
katana::EdgeShuffleTopology::MakeOriginalCopy(const katana::PropertyGraph* pg) {
  GraphTopology copy_topo = GraphTopology::Copy(pg->topology());

  GraphTopologyTypes::PropIndexVec edge_prop_indices;
  edge_prop_indices.allocateInterleaved(copy_topo.NumEdges());
  katana::ParallelSTL::iota(
      edge_prop_indices.begin(), edge_prop_indices.end(), Edge{0});

  return std::make_shared<EdgeShuffleTopology>(EdgeShuffleTopology{
      katana::RDGTopology::TransposeKind::kNo,
      katana::RDGTopology::EdgeSortKind::kAny,
      std::move(copy_topo.GetAdjIndices()), std::move(copy_topo.GetDests()),
      std::move(edge_prop_indices)});
}

std::shared_ptr<katana::EdgeShuffleTopology>
katana::EdgeShuffleTopology::Make(katana::RDGTopology* rdg_topo) {
  KATANA_LOG_DEBUG_ASSERT(rdg_topo);

  EdgeDestVec dests_copy;
  dests_copy.allocateInterleaved(rdg_topo->num_edges());
  AdjIndexVec adj_indices_copy;
  adj_indices_copy.allocateInterleaved(rdg_topo->num_nodes());
  PropIndexVec edge_prop_indices;
  edge_prop_indices.allocateInterleaved(rdg_topo->num_edges());

  if (rdg_topo->num_nodes() > 0) {
    katana::ParallelSTL::copy(
        &(rdg_topo->adj_indices()[0]),
        &(rdg_topo->adj_indices()[rdg_topo->num_nodes()]),
        adj_indices_copy.begin());
  }
  if (rdg_topo->num_edges() > 0) {
    katana::ParallelSTL::copy(
        &(rdg_topo->dests()[0]), &(rdg_topo->dests()[rdg_topo->num_edges()]),
        dests_copy.begin());

    katana::ParallelSTL::copy(
        &(rdg_topo->edge_index_to_property_index_map()[0]),
        &(rdg_topo->edge_index_to_property_index_map()[rdg_topo->num_edges()]),
        edge_prop_indices.begin());
  }

  // Since we copy the data we need out of the RDGTopology into our own arrays,
  // unbind the RDGTopologys file store to save memory.
  auto res = rdg_topo->unbind_file_storage();
  KATANA_LOG_ASSERT(res);

  std::shared_ptr<EdgeShuffleTopology> shuffle =
      std::make_shared<EdgeShuffleTopology>(EdgeShuffleTopology{
          rdg_topo->transpose_state(), rdg_topo->edge_sort_state(),
          std::move(adj_indices_copy), std::move(dests_copy),
          std::move(edge_prop_indices)});

  return shuffle;
}

katana::Result<katana::RDGTopology>
katana::EdgeShuffleTopology::ToRDGTopology() const {
  katana::RDGTopology topo = KATANA_CHECKED(katana::RDGTopology::Make(
      AdjData(), NumNodes(), DestData(), NumEdges(),
      katana::RDGTopology::TopologyKind::kEdgeShuffleTopology, tpose_state_,
      edge_sort_state_, edge_prop_indices_.data()));
  return katana::RDGTopology(std::move(topo));
}

katana::GraphTopologyTypes::edge_iterator
katana::EdgeShuffleTopology::FindEdge(
    const katana::GraphTopologyTypes::Node& src,
    const katana::GraphTopologyTypes::Node& dst) const noexcept {
  auto e_range = OutEdges(src);

  constexpr size_t kBinarySearchThreshold = 64;

  if (e_range.size() > kBinarySearchThreshold &&
      !has_edges_sorted_by(
          katana::RDGTopology::EdgeSortKind::kSortedByDestID)) {
    KATANA_WARN_ONCE(
        "FindEdge(): expect poor performance. Edges not sorted by Dest ID");
  }

  if (e_range.size() <= kBinarySearchThreshold) {
    auto iter = std::find_if(
        e_range.begin(), e_range.end(),
        [&](const GraphTopology::Edge& e) { return OutEdgeDst(e) == dst; });

    return iter;

  } else {
    auto iter = std::lower_bound(
        e_range.begin(), e_range.end(), dst,
        internal::EdgeDestComparator<EdgeShuffleTopology>{this});

    return OutEdgeDst(*iter) == dst ? iter : e_range.end();
  }
}

katana::GraphTopologyTypes::edges_range
katana::EdgeShuffleTopology::FindAllEdges(
    const katana::GraphTopologyTypes::Node& src,
    const katana::GraphTopologyTypes::Node& dst) const noexcept {
  auto e_range = OutEdges(src);
  if (e_range.empty()) {
    return e_range;
  }

  KATANA_LOG_VASSERT(
      !has_edges_sorted_by(katana::RDGTopology::EdgeSortKind::kSortedByDestID),
      "Must have edges sorted by kSortedByDestID");

  internal::EdgeDestComparator<EdgeShuffleTopology> comp{this};
  auto [first_it, last_it] =
      std::equal_range(e_range.begin(), e_range.end(), dst, comp);

  if (first_it == e_range.end() || OutEdgeDst(*first_it) != dst) {
    // return empty range
    return MakeStandardRange(e_range.end(), e_range.end());
  }

  auto ret_range = MakeStandardRange(first_it, last_it);
  for ([[maybe_unused]] auto e : ret_range) {
    KATANA_LOG_DEBUG_ASSERT(OutEdgeDst(e) == dst);
  }
  return ret_range;
}

void
katana::EdgeShuffleTopology::SortEdgesByDestID() noexcept {
  katana::do_all(
      katana::iterate(Nodes()),
      [&](Node node) {
        // get this node's first and last edge
        auto e_beg = *OutEdges(node).begin();
        auto e_end = *OutEdges(node).end();

        // get iterators to locations to sort in the vector
        auto begin_sort_iter = katana::make_zip_iterator(
            edge_prop_indices_.begin() + e_beg, GetDests().begin() + e_beg);

        auto end_sort_iter = katana::make_zip_iterator(
            edge_prop_indices_.begin() + e_end, GetDests().begin() + e_end);

        // rearrange vector indices based on how the destinations of this
        // graph will eventually be sorted sort function not based on vector
        // being passed, but rather the type and destination of the graph
        std::sort(
            begin_sort_iter, end_sort_iter,
            [&](const auto& tup1, const auto& tup2) {
              auto dst1 = std::get<1>(tup1);
              auto dst2 = std::get<1>(tup2);
              static_assert(
                  std::is_same_v<decltype(dst1), GraphTopology::Node>);
              static_assert(
                  std::is_same_v<decltype(dst2), GraphTopology::Node>);
              return dst1 < dst2;
            });

        KATANA_LOG_DEBUG_ASSERT(std::is_sorted(
            GetDests().begin() + e_beg, GetDests().begin() + e_end));
      },
      katana::steal(), katana::no_stats());
  // remember to update sort state
  edge_sort_state_ = katana::RDGTopology::EdgeSortKind::kSortedByDestID;
}

void
katana::EdgeShuffleTopology::SortEdgesByTypeThenDest(
    const PropertyGraph* pg) noexcept {
  katana::do_all(
      katana::iterate(Nodes()),
      [&](Node node) {
        // get this node's first and last edge
        auto e_beg = *OutEdges(node).begin();
        auto e_end = *OutEdges(node).end();

        // get iterators to locations to sort in the vector
        auto begin_sort_iter = katana::make_zip_iterator(
            edge_prop_indices_.begin() + e_beg, GetDests().begin() + e_beg);

        auto end_sort_iter = katana::make_zip_iterator(
            edge_prop_indices_.begin() + e_end, GetDests().begin() + e_end);

        // rearrange vector indices based on how the destinations of this
        // graph will eventually be sorted sort function not based on vector
        // being passed, but rather the type and destination of the graph
        std::sort(
            begin_sort_iter, end_sort_iter,
            [&](const auto& tup1, const auto& tup2) {
              // get edge type and destinations
              auto e1 = std::get<0>(tup1);
              auto e2 = std::get<0>(tup2);
              static_assert(
                  std::is_same_v<decltype(e1), GraphTopology::PropertyIndex>);
              static_assert(
                  std::is_same_v<decltype(e2), GraphTopology::PropertyIndex>);

              katana::EntityTypeID data1 =
                  pg->GetTypeOfEdgeFromPropertyIndex(e1);
              katana::EntityTypeID data2 =
                  pg->GetTypeOfEdgeFromPropertyIndex(e2);
              if (data1 != data2) {
                return data1 < data2;
              }

              auto dst1 = std::get<1>(tup1);
              auto dst2 = std::get<1>(tup2);
              static_assert(
                  std::is_same_v<decltype(dst1), GraphTopology::Node>);
              static_assert(
                  std::is_same_v<decltype(dst2), GraphTopology::Node>);
              return dst1 < dst2;
            });
      },
      katana::steal(), katana::no_stats());

  // remember to update sort state
  edge_sort_state_ = katana::RDGTopology::EdgeSortKind::kSortedByEdgeType;
}

void
katana::EdgeShuffleTopology::SortEdgesByDestType(
    const PropertyGraph*,
    const katana::GraphTopologyTypes::PropIndexVec&) noexcept {
  KATANA_LOG_FATAL("Not implemented yet");
}

std::shared_ptr<katana::ShuffleTopology>
katana::ShuffleTopology::MakeSortedByDegree(
    const PropertyGraph*,
    const katana::EdgeShuffleTopology& seed_topo) noexcept {
  auto cmp = [&](const auto& i1, const auto& i2) {
    auto d1 = seed_topo.OutDegree(i1);
    auto d2 = seed_topo.OutDegree(i2);
    // TODO(amber): Triangle-Counting needs degrees sorted in descending order. I
    // need to think of a way to specify in the interface whether degrees should be
    // sorted in ascending or descending order.
    // return d1 < d2;
    return d1 > d2;
  };

  return MakeNodeSortedTopo(
      seed_topo, cmp, katana::RDGTopology::NodeSortKind::kSortedByDegree);
}

std::shared_ptr<katana::ShuffleTopology>
katana::ShuffleTopology::MakeSortedByNodeType(
    const PropertyGraph* pg,
    const katana::EdgeShuffleTopology& seed_topo) noexcept {
  auto cmp = [&](const auto& i1, const auto& i2) {
    auto k1 = pg->GetTypeOfNode(i1);
    auto k2 = pg->GetTypeOfNode(i2);
    if (k1 == k2) {
      return i1 < i2;
    }
    return k1 < k2;
  };

  return MakeNodeSortedTopo(
      seed_topo, cmp, katana::RDGTopology::NodeSortKind::kSortedByNodeType);
}

std::shared_ptr<katana::ShuffleTopology>
katana::ShuffleTopology::Make(katana::RDGTopology* rdg_topo) {
  KATANA_LOG_DEBUG_ASSERT(rdg_topo);
  EdgeDestVec dests_copy;
  dests_copy.allocateInterleaved(rdg_topo->num_edges());
  AdjIndexVec adj_indices_copy;
  adj_indices_copy.allocateInterleaved(rdg_topo->num_nodes());
  PropIndexVec edge_prop_indices_copy;
  edge_prop_indices_copy.allocateInterleaved(rdg_topo->num_edges());
  PropIndexVec node_prop_indices_copy;
  node_prop_indices_copy.allocateInterleaved(rdg_topo->num_nodes());

  katana::ParallelSTL::copy(
      &(rdg_topo->adj_indices()[0]),
      &(rdg_topo->adj_indices()[rdg_topo->num_nodes()]),
      adj_indices_copy.begin());
  katana::ParallelSTL::copy(
      &(rdg_topo->dests()[0]), &(rdg_topo->dests()[rdg_topo->num_edges()]),
      dests_copy.begin());

  katana::ParallelSTL::copy(
      &(rdg_topo->edge_index_to_property_index_map()[0]),
      &(rdg_topo->edge_index_to_property_index_map()[rdg_topo->num_edges()]),
      edge_prop_indices_copy.begin());

  katana::ParallelSTL::copy(
      &(rdg_topo->node_index_to_property_index_map()[0]),
      &(rdg_topo->node_index_to_property_index_map()[rdg_topo->num_nodes()]),
      node_prop_indices_copy.begin());

  // Since we copy the data we need out of the RDGTopology into our own arrays,
  // unbind the RDGTopologys file store to save memory.
  auto res = rdg_topo->unbind_file_storage();
  KATANA_LOG_ASSERT(res);

  std::shared_ptr<ShuffleTopology> shuffle =
      std::make_shared<ShuffleTopology>(ShuffleTopology{
          rdg_topo->transpose_state(), rdg_topo->node_sort_state(),
          rdg_topo->edge_sort_state(), std::move(adj_indices_copy),
          std::move(node_prop_indices_copy), std::move(dests_copy),
          std::move(edge_prop_indices_copy)});

  return shuffle;
}

katana::Result<katana::RDGTopology>
katana::ShuffleTopology::ToRDGTopology() const {
  katana::RDGTopology topo = KATANA_CHECKED(katana::RDGTopology::Make(
      AdjData(), NumNodes(), DestData(), NumEdges(),
      katana::RDGTopology::TopologyKind::kShuffleTopology, transpose_state(),
      edge_sort_state(), node_sort_state(), edge_property_index_data(),
      node_prop_indices_.data()));
  return katana::RDGTopology(std::move(topo));
}

std::shared_ptr<katana::CondensedTypeIDMap>
katana::CondensedTypeIDMap::MakeFromEdgeTypes(
    const katana::PropertyGraph* pg) noexcept {
  TypeIDToIndexMap edge_type_to_index;
  IndexToTypeIDMap edge_index_to_type;

  katana::PerThreadStorage<katana::gstl::Set<katana::EntityTypeID>> edgeTypes;

  const auto& topo = pg->topology();

  katana::do_all(
      katana::iterate(Edge{0}, topo.NumEdges()),
      [&](const Edge& e) {
        katana::EntityTypeID type = pg->GetTypeOfEdgeFromPropertyIndex(e);
        edgeTypes.getLocal()->insert(type);
      },
      katana::no_stats());

  // ordered map
  std::set<katana::EntityTypeID> mergedSet;
  for (uint32_t i = 0; i < katana::activeThreads; ++i) {
    auto& edgeTypesSet = *edgeTypes.getRemote(i);
    for (auto edgeType : edgeTypesSet) {
      mergedSet.insert(edgeType);
    }
  }

  // unordered map
  uint32_t num_edge_types = 0u;
  for (const auto& edgeType : mergedSet) {
    edge_type_to_index[edgeType] = num_edge_types++;
    edge_index_to_type.emplace_back(edgeType);
  }

  // TODO(amber): introduce a per-thread-container type that frees memory
  // correctly
  katana::on_each([&](unsigned, unsigned) {
    // free up memory by resetting
    *edgeTypes.getLocal() = gstl::Set<katana::EntityTypeID>();
  });

  return std::make_shared<CondensedTypeIDMap>(CondensedTypeIDMap{
      std::move(edge_type_to_index), std::move(edge_index_to_type)});
}

katana::EdgeTypeAwareTopology::~EdgeTypeAwareTopology() = default;

katana::EdgeTypeAwareTopology::AdjIndexVec
katana::EdgeTypeAwareTopology::CreatePerEdgeTypeAdjacencyIndex(
    const PropertyGraph& pg, const CondensedTypeIDMap& edge_type_index,
    const EdgeShuffleTopology& e_topo) noexcept {
  if (e_topo.empty()) {
    KATANA_LOG_VASSERT(
        e_topo.NumEdges() == 0, "Found graph with edges but no nodes");
    return AdjIndexVec{};
  }

  if (edge_type_index.num_unique_types() == 0) {
    KATANA_LOG_VASSERT(
        e_topo.NumEdges() == 0, "Found graph with edges but no edge types");
    // Graph has some nodes but no edges.
    return AdjIndexVec{};
  }

  const size_t sz = e_topo.NumNodes() * edge_type_index.num_unique_types();
  AdjIndexVec adj_indices;
  adj_indices.allocateInterleaved(sz);

  katana::do_all(
      katana::iterate(e_topo.Nodes()),
      [&](Node N) {
        auto offset = N * edge_type_index.num_unique_types();
        uint32_t index = 0;
        for (auto e : e_topo.OutEdges(N)) {
          // Since we sort the edges, we must use the
          // edge_property_index because EdgeShuffleTopology rearranges the edges
          const auto type = pg.GetTypeOfEdgeFromPropertyIndex(
              e_topo.GetEdgePropertyIndexFromOutEdge(e));
          while (type != edge_type_index.GetType(index)) {
            adj_indices[offset + index] = e;
            index++;
            KATANA_LOG_DEBUG_ASSERT(index < edge_type_index.num_unique_types());
          }
        }
        auto e = *e_topo.OutEdges(N).end();
        while (index < edge_type_index.num_unique_types()) {
          adj_indices[offset + index] = e;
          index++;
        }
      },
      katana::no_stats(), katana::steal());

  return adj_indices;
}

std::shared_ptr<katana::EdgeTypeAwareTopology>
katana::EdgeTypeAwareTopology::MakeFrom(
    const katana::PropertyGraph* pg,
    std::shared_ptr<const CondensedTypeIDMap> edge_type_index,
    EdgeShuffleTopology&& e_topo) noexcept {
  KATANA_LOG_DEBUG_ASSERT(e_topo.has_edges_sorted_by(
      katana::RDGTopology::EdgeSortKind::kSortedByEdgeType));

  KATANA_LOG_DEBUG_ASSERT(e_topo.NumEdges() == pg->topology().NumEdges());

  AdjIndexVec per_type_adj_indices =
      CreatePerEdgeTypeAdjacencyIndex(*pg, *edge_type_index, e_topo);

  return std::make_shared<EdgeTypeAwareTopology>(EdgeTypeAwareTopology{
      std::move(e_topo), std::move(edge_type_index),
      std::move(per_type_adj_indices)});
}

katana::Result<katana::RDGTopology>
katana::EdgeTypeAwareTopology::ToRDGTopology() const {
  katana::RDGTopology topo = KATANA_CHECKED(katana::RDGTopology::Make(
      per_type_adj_indices_.data(), NumNodes(), Base::DestData(), NumEdges(),
      katana::RDGTopology::TopologyKind::kEdgeTypeAwareTopology,
      transpose_state(), edge_sort_state(), Base::edge_property_index_data(),
      edge_type_index_->num_unique_types(),
      edge_type_index_->index_to_type_map_data()));

  return katana::RDGTopology(std::move(topo));
}

std::shared_ptr<katana::EdgeTypeAwareTopology>
katana::EdgeTypeAwareTopology::Make(
    katana::RDGTopology* rdg_topo,
    std::shared_ptr<const CondensedTypeIDMap> edge_type_index,
    EdgeShuffleTopology&& e_topo) {
  KATANA_LOG_DEBUG_ASSERT(rdg_topo);

  KATANA_LOG_ASSERT(
      rdg_topo->edge_sort_state() ==
      katana::RDGTopology::EdgeSortKind::kSortedByEdgeType);

  KATANA_LOG_DEBUG_ASSERT(e_topo.has_edges_sorted_by(
      katana::RDGTopology::EdgeSortKind::kSortedByEdgeType));

  KATANA_LOG_VASSERT(
      edge_type_index->index_to_type_map_matches(
          rdg_topo->edge_condensed_type_id_map_size(),
          rdg_topo->edge_condensed_type_id_map()) &&
          e_topo.NumEdges() == rdg_topo->num_edges() &&
          e_topo.NumNodes() == rdg_topo->num_nodes(),
      "tried to load out of date EdgeTypeAwareTopology; on disk topologies "
      "must be invalidated when updates occur");

  AdjIndexVec per_type_adj_indices;
  size_t sz = rdg_topo->num_nodes() * edge_type_index->num_unique_types();
  per_type_adj_indices.allocateInterleaved(sz);

  katana::ParallelSTL::copy(
      &(rdg_topo->adj_indices()[0]), &(rdg_topo->adj_indices()[sz]),
      per_type_adj_indices.begin());

  // Since we copy the data we need out of the RDGTopology into our own arrays,
  // unbind the RDGTopologys file store to save memory.
  auto res = rdg_topo->unbind_file_storage();
  KATANA_LOG_ASSERT(res);

  return std::make_shared<EdgeTypeAwareTopology>(EdgeTypeAwareTopology{
      std::move(e_topo), std::move(edge_type_index),
      std::move(per_type_adj_indices)});
}

/// This function converts a bitset to a bitmask
void
katana::ProjectedTopology::FillBitMask(
    size_t num_elements, const katana::DynamicBitset& bitset,
    katana::NUMAArray<uint8_t>* bitmask) {
  uint32_t num_bytes = (num_elements + 7) / 8;

  // TODO(udit) find another way to do the following
  // as it is prone to errors
  katana::do_all(katana::iterate(uint32_t{0}, num_bytes), [&](uint32_t i) {
    auto start = i * 8;
    auto end = (i + 1) * 8;
    end = (end > num_elements) ? num_elements : end;
    uint8_t val{0};
    while (start != end) {
      if (bitset.test(start)) {
        uint8_t bit_offset{1};
        bit_offset <<= (start % 8);
        val = val | bit_offset;
      }
      start++;
    }
    (*bitmask)[i] = val;
  });
}

std::shared_ptr<katana::ProjectedTopology>
katana::ProjectedTopology::CreateEmptyEdgeProjectedTopology(
    const katana::PropertyGraph* pg, uint32_t num_new_nodes,
    const katana::DynamicBitset& bitset) {
  const auto& topology = pg->topology();

  katana::NUMAArray<Edge> out_indices;
  out_indices.allocateInterleaved(num_new_nodes);

  katana::NUMAArray<Node> out_dests;
  katana::NUMAArray<Node> original_to_projected_nodes_mapping;
  original_to_projected_nodes_mapping.allocateInterleaved(topology.NumNodes());
  katana::ParallelSTL::fill(
      original_to_projected_nodes_mapping.begin(),
      original_to_projected_nodes_mapping.end(),
      static_cast<Node>(topology.NumNodes()));

  katana::NUMAArray<Node> projected_to_original_nodes_mapping;
  projected_to_original_nodes_mapping.allocateInterleaved(num_new_nodes);

  katana::NUMAArray<Edge> original_to_projected_edges_mapping;
  katana::NUMAArray<Edge> projected_to_original_edges_mapping;

  original_to_projected_edges_mapping.allocateInterleaved(topology.NumEdges());
  katana::ParallelSTL::fill(
      original_to_projected_edges_mapping.begin(),
      original_to_projected_edges_mapping.end(), Edge{topology.NumEdges()});

  NUMAArray<uint8_t> node_bitmask;
  node_bitmask.allocateInterleaved((topology.NumNodes() + 7) / 8);

  FillBitMask(topology.NumNodes(), bitset, &node_bitmask);

  NUMAArray<uint8_t> edge_bitmask;
  edge_bitmask.allocateInterleaved((topology.NumEdges() + 7) / 8);

  return std::make_shared<katana::ProjectedTopology>(katana::ProjectedTopology{
      std::move(out_indices), std::move(out_dests),
      std::move(original_to_projected_nodes_mapping),
      std::move(projected_to_original_nodes_mapping),
      std::move(original_to_projected_edges_mapping),
      std::move(projected_to_original_edges_mapping), std::move(node_bitmask),
      std::move(edge_bitmask)});
}

std::shared_ptr<katana::ProjectedTopology>
katana::ProjectedTopology::CreateEmptyProjectedTopology(
    const katana::PropertyGraph* pg, const katana::DynamicBitset& bitset) {
  return CreateEmptyEdgeProjectedTopology(pg, 0, bitset);
}

std::shared_ptr<katana::ProjectedTopology>
katana::ProjectedTopology::MakeTypeProjectedTopology(
    const katana::PropertyGraph* pg, const std::vector<std::string>& node_types,
    const std::vector<std::string>& edge_types) {
  KATANA_LOG_DEBUG_ASSERT(pg);

  const auto& topology = pg->topology();
  if (topology.empty()) {
    return std::make_shared<ProjectedTopology>(ProjectedTopology());
  }

  // calculate number of new nodes
  uint32_t num_new_nodes = 0;
  uint32_t num_new_edges = 0;

  katana::DynamicBitset bitset_nodes;
  bitset_nodes.resize(topology.NumNodes());

  NUMAArray<Node> original_to_projected_nodes_mapping;
  original_to_projected_nodes_mapping.allocateInterleaved(topology.NumNodes());

  if (node_types.empty()) {
    num_new_nodes = topology.NumNodes();
    // set all nodes
    katana::do_all(katana::iterate(topology.Nodes()), [&](auto src) {
      bitset_nodes.set(src);
      original_to_projected_nodes_mapping[src] = 1;
    });
  } else {
    katana::ParallelSTL::fill(
        original_to_projected_nodes_mapping.begin(),
        original_to_projected_nodes_mapping.end(), Node{0});

    std::set<katana::EntityTypeID> node_entity_type_ids;

    for (auto node_type : node_types) {
      auto entity_type_id = pg->GetNodeEntityTypeID(node_type);
      node_entity_type_ids.insert(entity_type_id);
    }

    katana::GAccumulator<uint32_t> accum_num_new_nodes;

    katana::do_all(katana::iterate(topology.Nodes()), [&](auto src) {
      for (auto type : node_entity_type_ids) {
        if (pg->DoesNodeHaveType(src, type)) {
          accum_num_new_nodes += 1;
          bitset_nodes.set(src);
          // this sets the correspondign entry in the array to 1
          // will perform a prefix sum on this array later on
          original_to_projected_nodes_mapping[src] = 1;
          return;
        }
      }
    });
    num_new_nodes = accum_num_new_nodes.reduce();

    if (num_new_nodes == 0) {
      // no nodes selected;
      // return empty graph
      return CreateEmptyProjectedTopology(pg, bitset_nodes);
    }
  }

  // fill old to new nodes mapping
  katana::ParallelSTL::partial_sum(
      original_to_projected_nodes_mapping.begin(),
      original_to_projected_nodes_mapping.end(),
      original_to_projected_nodes_mapping.begin());

  NUMAArray<Node> projected_to_original_nodes_mapping;
  projected_to_original_nodes_mapping.allocateInterleaved(num_new_nodes);

  uint32_t num_nodes_bytes = (topology.NumNodes() + 7) / 8;

  NUMAArray<uint8_t> node_bitmask;
  node_bitmask.allocateInterleaved(num_nodes_bytes);

  katana::do_all(katana::iterate(topology.Nodes()), [&](auto src) {
    if (bitset_nodes.test(src)) {
      original_to_projected_nodes_mapping[src]--;
      projected_to_original_nodes_mapping
          [original_to_projected_nodes_mapping[src]] = src;
    } else {
      original_to_projected_nodes_mapping[src] = topology.NumNodes();
    }
  });

  FillBitMask(topology.NumNodes(), bitset_nodes, &node_bitmask);

  // calculate number of new edges
  katana::DynamicBitset bitset_edges;
  bitset_edges.resize(topology.NumEdges());

  NUMAArray<Edge> out_indices;
  out_indices.allocateInterleaved(num_new_nodes);

  // initializes the edge-index array to all zeros
  katana::ParallelSTL::fill(out_indices.begin(), out_indices.end(), Edge{0});

  if (edge_types.empty()) {
    katana::GAccumulator<uint32_t> accum_num_new_edges;
    // set all edges incident to projected nodes
    katana::do_all(
        katana::iterate(Node{0}, Node{num_new_nodes}),
        [&](auto src) {
          auto old_src = projected_to_original_nodes_mapping[src];
          for (Edge e : topology.OutEdges(old_src)) {
            auto dest = topology.OutEdgeDst(e);
            if (bitset_nodes.test(dest)) {
              bitset_edges.set(e);
              out_indices[src] += 1;
              accum_num_new_edges += 1;
            }
          }
        },
        katana::steal());

    num_new_edges = accum_num_new_edges.reduce();
  } else {
    std::set<katana::EntityTypeID> edge_entity_type_ids;

    for (auto edge_type : edge_types) {
      auto entity_type_id = pg->GetEdgeEntityTypeID(edge_type);
      edge_entity_type_ids.insert(entity_type_id);
    }

    katana::GAccumulator<uint32_t> accum_num_new_edges;

    katana::do_all(
        katana::iterate(Node{0}, Node{num_new_nodes}),
        [&](auto src) {
          auto old_src = projected_to_original_nodes_mapping[src];

          for (Edge e : topology.OutEdges(old_src)) {
            auto dest = topology.OutEdgeDst(e);
            if (bitset_nodes.test(dest)) {
              for (auto type : edge_entity_type_ids) {
                if (pg->DoesEdgeHaveTypeFromTopoIndex(e, type)) {
                  accum_num_new_edges += 1;
                  bitset_edges.set(e);
                  out_indices[src] += 1;
                  break;
                }
              }
            }
          }
        },
        katana::steal());

    num_new_edges = accum_num_new_edges.reduce();

    if (num_new_edges == 0) {
      // no edge selected
      // return empty graph with only selected nodes
      return CreateEmptyEdgeProjectedTopology(pg, num_new_nodes, bitset_nodes);
    }
  }

  // Prefix sum calculation of the edge index array
  katana::ParallelSTL::partial_sum(
      out_indices.begin(), out_indices.end(), out_indices.begin());

  NUMAArray<Edge> out_dests_offset;
  out_dests_offset.allocateInterleaved(num_new_nodes);

  // temporary buffer for storing the starting point of each node's
  // adjacency
  out_dests_offset[0] = 0;
  katana::do_all(
      katana::iterate(Node{1}, Node{num_new_nodes}),
      [&](Node n) { out_dests_offset[n] = out_indices[n - 1]; },
      katana::no_stats());

  NUMAArray<Node> out_dests;
  NUMAArray<Edge> original_to_projected_edges_mapping;
  NUMAArray<Edge> projected_to_original_edges_mapping;
  NUMAArray<uint8_t> edge_bitmask;

  out_dests.allocateInterleaved(num_new_edges);
  original_to_projected_edges_mapping.allocateInterleaved(topology.NumEdges());
  projected_to_original_edges_mapping.allocateInterleaved(num_new_edges);
  edge_bitmask.allocateInterleaved((topology.NumEdges() + 7) / 8);

  // Update out_dests with the new destination ids
  katana::do_all(
      katana::iterate(Node{0}, Node{num_new_nodes}),
      [&](Node n) {
        auto src = projected_to_original_nodes_mapping[n];

        for (Edge e : topology.OutEdges(src)) {
          if (bitset_edges.test(e)) {
            auto e_new = out_dests_offset[n];
            out_dests_offset[n]++;

            auto dest = topology.OutEdgeDst(e);
            dest = original_to_projected_nodes_mapping[dest];
            out_dests[e_new] = dest;

            original_to_projected_edges_mapping[e] = e_new;
            projected_to_original_edges_mapping[e_new] = e;
          }
        }
      },
      katana::steal());

  katana::do_all(katana::iterate(topology.OutEdges()), [&](auto edge) {
    if (!bitset_edges.test(edge)) {
      original_to_projected_edges_mapping[edge] = topology.NumEdges();
    }
  });

  FillBitMask(topology.NumEdges(), bitset_edges, &edge_bitmask);
  return std::make_shared<ProjectedTopology>(ProjectedTopology{
      std::move(out_indices), std::move(out_dests),
      std::move(original_to_projected_nodes_mapping),
      std::move(projected_to_original_nodes_mapping),
      std::move(original_to_projected_edges_mapping),
      std::move(projected_to_original_edges_mapping), std::move(node_bitmask),
      std::move(edge_bitmask)});
}

const katana::GraphTopology&
katana::PGViewCache::GetDefaultTopologyRef() const noexcept {
  return *original_topo_;
}

std::shared_ptr<katana::GraphTopology>
katana::PGViewCache::GetDefaultTopology() const noexcept {
  return original_topo_;
}

bool
katana::PGViewCache::ReseatDefaultTopo(
    const std::shared_ptr<GraphTopology>& other) noexcept {
  // We check for the original sort state to avoid doing this every time a new
  // edge shuffle topo is cached.
  if (original_topo_->edge_sort_state() !=
      katana::RDGTopology::EdgeSortKind::kAny) {
    return false;
  }

  original_topo_ = other;
  return true;
}

void
katana::PGViewCache::DropAllTopologies() noexcept {
  original_topo_ = std::make_shared<katana::GraphTopology>();

  edge_shuff_topos_.clear();
  fully_shuff_topos_.clear();
  edge_type_aware_topos_.clear();
  edge_type_id_map_.reset();
  projected_topos_.reset();
}

std::shared_ptr<katana::CondensedTypeIDMap>
katana::PGViewCache::BuildOrGetEdgeTypeIndex(
    const katana::PropertyGraph* pg) noexcept {
  if (edge_type_id_map_ && edge_type_id_map_->is_valid()) {
    return edge_type_id_map_;
  }

  edge_type_id_map_ = CondensedTypeIDMap::MakeFromEdgeTypes(pg);
  KATANA_LOG_DEBUG_ASSERT(edge_type_id_map_);
  return edge_type_id_map_;
};

template <typename Topo>
[[maybe_unused]] bool
CheckTopology(const katana::PropertyGraph* pg, const Topo* t) noexcept {
  return (pg->NumNodes() == t->NumNodes()) && (pg->NumEdges() == t->NumEdges());
}

std::shared_ptr<katana::EdgeShuffleTopology>
katana::PGViewCache::BuildOrGetEdgeShuffTopo(
    katana::PropertyGraph* pg,
    const katana::RDGTopology::TransposeKind& tpose_kind,
    const katana::RDGTopology::EdgeSortKind& sort_kind) noexcept {
  return BuildOrGetEdgeShuffTopoImpl(pg, tpose_kind, sort_kind, false);
}

std::shared_ptr<katana::EdgeShuffleTopology>
katana::PGViewCache::PopEdgeShuffTopo(
    katana::PropertyGraph* pg,
    const katana::RDGTopology::TransposeKind& tpose_kind,
    const katana::RDGTopology::EdgeSortKind& sort_kind) noexcept {
  return BuildOrGetEdgeShuffTopoImpl(pg, tpose_kind, sort_kind, true);
}

std::shared_ptr<katana::EdgeShuffleTopology>
katana::PGViewCache::BuildOrGetEdgeShuffTopoImpl(
    katana::PropertyGraph* pg,
    const katana::RDGTopology::TransposeKind& tpose_kind,
    const katana::RDGTopology::EdgeSortKind& sort_kind, bool pop) noexcept {
  // Try to find a matching topology in the cache.
  auto pred = [&](const auto& topo_ptr) {
    return topo_ptr->is_valid() && topo_ptr->has_transpose_state(tpose_kind) &&
           topo_ptr->has_edges_sorted_by(sort_kind);
  };
  auto it =
      std::find_if(edge_shuff_topos_.begin(), edge_shuff_topos_.end(), pred);

  if (it != edge_shuff_topos_.end()) {
    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, it->get()));
    if (pop) {
      auto topo = *it;
      edge_shuff_topos_.erase(it);
      return topo;
    } else {
      return *it;
    }
  }

  // Then in edge type aware topologies. We don't pop from it.
  if (sort_kind == katana::RDGTopology::EdgeSortKind::kSortedByEdgeType) {
    auto it = std::find_if(
        edge_type_aware_topos_.begin(), edge_type_aware_topos_.end(), pred);
    if (it != edge_type_aware_topos_.end()) {
      KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, it->get()));
      return *it;
    }
  }

  // No matching topology in cache, see if we have it in storage
  katana::RDGTopology shadow = katana::RDGTopology::MakeShadow(
      katana::RDGTopology::TopologyKind::kEdgeShuffleTopology, tpose_kind,
      sort_kind, katana::RDGTopology::NodeSortKind::kAny);

  auto res = pg->LoadTopology(std::move(shadow));
  auto new_topo = (!res) ? EdgeShuffleTopology::Make(pg, tpose_kind, sort_kind)
                         : EdgeShuffleTopology::Make(res.value());
  KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, new_topo.get()));

  if (pop) {
    return new_topo;
  } else {
    edge_shuff_topos_.emplace_back(std::move(new_topo));
    return edge_shuff_topos_.back();
  }
}

std::shared_ptr<katana::ShuffleTopology>
katana::PGViewCache::BuildOrGetShuffTopo(
    katana::PropertyGraph* pg,
    const katana::RDGTopology::TransposeKind& tpose_kind,
    const katana::RDGTopology::NodeSortKind& node_sort_todo,
    const katana::RDGTopology::EdgeSortKind& edge_sort_todo) noexcept {
  // try to find a matching topology in the cache
  auto pred = [&](const auto& topo_ptr) {
    return topo_ptr->is_valid() && topo_ptr->has_transpose_state(tpose_kind) &&
           topo_ptr->has_edges_sorted_by(edge_sort_todo) &&
           topo_ptr->has_nodes_sorted_by(node_sort_todo);
  };

  auto it =
      std::find_if(fully_shuff_topos_.begin(), fully_shuff_topos_.end(), pred);

  if (it != fully_shuff_topos_.end()) {
    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, it->get()));
    return *it;
  } else {
    // no matching topology in cache, see if we have it in storage

    katana::RDGTopology shadow = katana::RDGTopology::MakeShadow(
        katana::RDGTopology::TopologyKind::kShuffleTopology, tpose_kind,
        edge_sort_todo, node_sort_todo);
    auto res = pg->LoadTopology(std::move(shadow));

    if (!res) {
      // no matching topology in cache or storage, generate it

      // EdgeShuffleTopology e_topo below is going to serve as a seed for
      // ShuffleTopology, so we only care about transpose state, and not the sort
      // state. Because, when creating ShuffleTopology, once we shuffle the nodes, we
      // will need to re-sort the edges even if they were already sorted
      auto e_topo = BuildOrGetEdgeShuffTopo(
          pg, tpose_kind, katana::RDGTopology::EdgeSortKind::kAny);
      KATANA_LOG_DEBUG_ASSERT(e_topo->has_transpose_state(tpose_kind));

      fully_shuff_topos_.emplace_back(ShuffleTopology::MakeFromTopo(
          pg, *e_topo, node_sort_todo, edge_sort_todo));

    } else {
      // found matching topology in storage
      katana::RDGTopology* topo = res.value();
      fully_shuff_topos_.emplace_back(katana::ShuffleTopology::Make(topo));
    }

    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, fully_shuff_topos_.back().get()));
    return fully_shuff_topos_.back();
  }
}

std::shared_ptr<katana::EdgeTypeAwareTopology>
katana::PGViewCache::BuildOrGetEdgeTypeAwareTopo(
    katana::PropertyGraph* pg,
    const katana::RDGTopology::TransposeKind& tpose_kind) noexcept {
  // try to find a matching topology in the cache
  auto pred = [&](const auto& topo_ptr) {
    return topo_ptr->is_valid() && topo_ptr->has_transpose_state(tpose_kind);
  };
  auto it = std::find_if(
      edge_type_aware_topos_.begin(), edge_type_aware_topos_.end(), pred);

  if (it != edge_type_aware_topos_.end()) {
    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, it->get()));
    return *it;
  } else {
    // no matching topology in cache, see if we have it in storage
    katana::RDGTopology shadow = katana::RDGTopology::MakeShadow(
        katana::RDGTopology::TopologyKind::kEdgeTypeAwareTopology, tpose_kind,
        katana::RDGTopology::EdgeSortKind::kSortedByEdgeType,
        katana::RDGTopology::NodeSortKind::kAny);
    auto res = pg->LoadTopology(std::move(shadow));

    // In either generation, or loading, the EdgeTypeAwareTopology depends on an EdgeShuffleTopology.
    // This call does NOT cache the resulting edge shuffled topology.
    auto sorted_topo = PopEdgeShuffTopo(
        pg, tpose_kind, katana::RDGTopology::EdgeSortKind::kSortedByEdgeType);

    // There are two use cases for the EdgeTypeIndex, either we:
    // Are generating an EdgeTypeAwareTopology, and need the EdgeTypeIndex
    // Are loading an EdgeTypeAwareTopology from storage, and need to confirm
    // the EdgeTypeIndex in storage matches the one we have.
    // If it doesn't match, then the EdgeTypeAwareTopology on storage is out of date and cannot be used
    auto edge_type_index = BuildOrGetEdgeTypeIndex(pg);

    if (res) {
      // found matching topology in storage
      katana::RDGTopology* rdg_topo = res.value();

      edge_type_aware_topos_.emplace_back(katana::EdgeTypeAwareTopology::Make(
          rdg_topo, std::move(edge_type_index), std::move(*sorted_topo)));
    } else {
      // no matching topology in cache or storage, generate it
      edge_type_aware_topos_.emplace_back(EdgeTypeAwareTopology::MakeFrom(
          pg, std::move(edge_type_index), std::move(*sorted_topo)));
    }

    KATANA_LOG_DEBUG_ASSERT(
        CheckTopology(pg, edge_type_aware_topos_.back().get()));

    return edge_type_aware_topos_.back();
  }
}

std::shared_ptr<katana::ProjectedTopology>
katana::PGViewCache::BuildOrGetProjectedGraphTopo(
    const PropertyGraph* pg, const std::vector<std::string>& node_types,
    const std::vector<std::string>& edge_types) noexcept {
  if (projected_topos_) {
    return projected_topos_;
  }

  projected_topos_ =
      ProjectedTopology::MakeTypeProjectedTopology(pg, node_types, edge_types);
  KATANA_LOG_DEBUG_ASSERT(projected_topos_);
  return projected_topos_;
}

katana::Result<std::vector<katana::RDGTopology>>
katana::PGViewCache::ToRDGTopology() {
  std::vector<katana::RDGTopology> rdg_topos;

  for (size_t i = 0; i < edge_shuff_topos_.size(); i++) {
    katana::RDGTopology topo =
        KATANA_CHECKED(edge_shuff_topos_[i]->ToRDGTopology());
    rdg_topos.emplace_back(std::move(topo));
  }

  for (size_t i = 0; i < fully_shuff_topos_.size(); i++) {
    katana::RDGTopology topo =
        KATANA_CHECKED(fully_shuff_topos_[i]->ToRDGTopology());
    rdg_topos.emplace_back(std::move(topo));
  }

  for (size_t i = 0; i < edge_type_aware_topos_.size(); i++) {
    katana::RDGTopology topo =
        KATANA_CHECKED(edge_type_aware_topos_[i]->ToRDGTopology());
    rdg_topos.emplace_back(std::move(topo));
  }

  return std::vector<katana::RDGTopology>(std::move(rdg_topos));
}

katana::GraphTopology
katana::CreateUniformRandomTopology(
    const size_t num_nodes, const size_t edges_per_node) noexcept {
  KATANA_LOG_ASSERT(edges_per_node > 0);
  if (num_nodes == 0) {
    return GraphTopology{};
  }
  KATANA_LOG_ASSERT(edges_per_node <= num_nodes);

  GraphTopology::AdjIndexVec adj_indices;
  adj_indices.allocateInterleaved(num_nodes);
  // give each node edges_per_node neighbors
  katana::ParallelSTL::fill(
      adj_indices.begin(), adj_indices.end(),
      GraphTopology::Edge{edges_per_node});
  katana::ParallelSTL::partial_sum(
      adj_indices.begin(), adj_indices.end(), adj_indices.begin());

  const size_t num_edges = num_nodes * edges_per_node;
  KATANA_LOG_ASSERT(
      adj_indices.size() > 0 &&
      adj_indices[adj_indices.size() - 1] == num_edges);

  GraphTopology::EdgeDestVec dests;
  dests.allocateInterleaved(num_edges);
  // TODO(amber): Write a parallel version of GenerateUniformRandomSequence
  katana::GenerateUniformRandomSequence(
      dests.begin(), dests.end(), GraphTopology::Node{0},
      static_cast<GraphTopology::Node>(num_nodes - 1));

  return GraphTopology{std::move(adj_indices), std::move(dests)};
}

//////////////////////////////////////
// PropertyGraphView Make methods   //
//////////////////////////////////////

// Default view

template <>
std::unique_ptr<katana::internal::PGViewDefault>
katana::internal::PGViewDefault::Make(
    std::shared_ptr<PropertyGraph> pg) noexcept {
  auto topo = pg->GetPGViewCache()->GetDefaultTopology();
  return std::make_unique<katana::internal::PGViewDefault>(
      std::move(pg), katana::internal::DefaultPGTopology(std::move(topo)));
}

// Transposed view

template <>
std::unique_ptr<katana::internal::PGViewTransposed>
katana::internal::PGViewTransposed::Make(
    std::shared_ptr<PropertyGraph> pg) noexcept {
  auto transposed_topo = pg->GetPGViewCache()->BuildOrGetEdgeShuffTopo(
      pg.get(), katana::RDGTopology::TransposeKind::kYes,
      katana::RDGTopology::EdgeSortKind::kAny);
  return std::make_unique<katana::internal::PGViewTransposed>(
      std::move(pg),
      katana::internal::TransposedTopology(std::move(transposed_topo)));
}

// Edges sorted by destination view

template <>
std::unique_ptr<katana::internal::PGViewEdgesSortedByDestID>
katana::internal::PGViewEdgesSortedByDestID::Make(
    std::shared_ptr<PropertyGraph> pg) noexcept {
  auto sorted_topo = pg->GetPGViewCache()->BuildOrGetEdgeShuffTopo(
      pg.get(), katana::RDGTopology::TransposeKind::kNo,
      katana::RDGTopology::EdgeSortKind::kSortedByDestID);
  return std::make_unique<katana::internal::PGViewEdgesSortedByDestID>(
      std::move(pg),
      katana::internal::EdgesSortedByDestTopology(std::move(sorted_topo)));
}

// Nodes sorted by degree, edges sorted by destination view

template <>
std::unique_ptr<katana::internal::PGViewNodesSortedByDegreeEdgesSortedByDestID>
katana::internal::PGViewNodesSortedByDegreeEdgesSortedByDestID::Make(
    std::shared_ptr<PropertyGraph> pg) noexcept {
  auto sorted_topo = pg->GetPGViewCache()->BuildOrGetShuffTopo(
      pg.get(), katana::RDGTopology::TransposeKind::kNo,
      katana::RDGTopology::NodeSortKind::kSortedByDegree,
      katana::RDGTopology::EdgeSortKind::kSortedByDestID);
  return std::make_unique<
      katana::internal::PGViewNodesSortedByDegreeEdgesSortedByDestID>(
      std::move(pg),
      katana::internal::NodesSortedByDegreeEdgesSortedByDestIDTopology(
          std::move(sorted_topo)));
}

// Bidirectional view

template <>
std::unique_ptr<katana::internal::PGViewBiDirectional>
katana::internal::PGViewBiDirectional::Make(
    std::shared_ptr<PropertyGraph> pg) noexcept {
  auto tpose_topo = pg->GetPGViewCache()->BuildOrGetEdgeShuffTopo(
      pg.get(), katana::RDGTopology::TransposeKind::kYes,
      katana::RDGTopology::EdgeSortKind::kAny);
  auto bidir_topo = katana::internal::SimpleBiDirTopology(
      pg->GetPGViewCache()->GetDefaultTopology(), std::move(tpose_topo));
  return std::make_unique<katana::internal::PGViewBiDirectional>(
      std::move(pg), std::move(bidir_topo));
}

// Undirected view

template <>
std::unique_ptr<katana::internal::PGViewUnDirected>
katana::internal::PGViewUnDirected::Make(
    std::shared_ptr<PropertyGraph> pg) noexcept {
  auto tpose_topo = pg->GetPGViewCache()->BuildOrGetEdgeShuffTopo(
      pg.get(), katana::RDGTopology::TransposeKind::kYes,
      katana::RDGTopology::EdgeSortKind::kAny);
  auto undir_topo = katana::internal::UndirectedTopology{
      pg->GetPGViewCache()->GetDefaultTopology(), tpose_topo};

  return std::make_unique<katana::internal::PGViewUnDirected>(
      std::move(pg), std::move(undir_topo));
}

// Edge type aware bidirectional view

template <>
std::unique_ptr<katana::internal::PGViewEdgeTypeAwareBiDir>
katana::internal::PGViewEdgeTypeAwareBiDir::Make(
    std::shared_ptr<PropertyGraph> pg) noexcept {
  auto out_topo = pg->GetPGViewCache()->BuildOrGetEdgeTypeAwareTopo(
      pg.get(), katana::RDGTopology::TransposeKind::kNo);
  auto in_topo = pg->GetPGViewCache()->BuildOrGetEdgeTypeAwareTopo(
      pg.get(), katana::RDGTopology::TransposeKind::kYes);

  // The new topology can replace the defaut one.
  pg->GetPGViewCache()->ReseatDefaultTopo(out_topo);

  return std::make_unique<katana::internal::PGViewEdgeTypeAwareBiDir>(
      std::move(pg),
      EdgeTypeAwareBiDirTopology(std::move(out_topo), std::move(in_topo)));
}

// Projected view

std::unique_ptr<katana::internal::PGViewProjectedGraph>
katana::internal::PGViewProjectedGraph::Make(
    std::shared_ptr<PropertyGraph> pg,
    const std::vector<std::string>& node_types,
    const std::vector<std::string>& edge_types) noexcept {
  auto topo = pg->GetPGViewCache()->BuildOrGetProjectedGraphTopo(
      pg.get(), node_types, edge_types);

  return std::make_unique<katana::internal::PGViewProjectedGraph>(
      std::move(pg), std::move(topo));
}

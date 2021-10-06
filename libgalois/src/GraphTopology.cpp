#include "katana/GraphTopology.h"

#include <iostream>

#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/Random.h"

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
}

katana::GraphTopology
katana::GraphTopology::Copy(const GraphTopology& that) noexcept {
  return katana::GraphTopology(
      that.adj_indices_.data(), that.adj_indices_.size(), that.dests_.data(),
      that.dests_.size());
}

std::unique_ptr<katana::ShuffleTopology>
katana::ShuffleTopology::MakeFrom(
    const PropertyGraph*, const katana::EdgeShuffleTopology&) noexcept {
  KATANA_LOG_FATAL("Not implemented yet");
  std::unique_ptr<ShuffleTopology> ret;
  return ret;
}

std::unique_ptr<katana::EdgeShuffleTopology>
katana::EdgeShuffleTopology::MakeTransposeCopy(
    const katana::PropertyGraph* pg) {
  KATANA_LOG_DEBUG_ASSERT(pg);

  const auto& topology = pg->topology();
  if (topology.empty()) {
    EdgeShuffleTopology et;
    et.tpose_state_ = TransposeKind::kYes;
    return std::make_unique<EdgeShuffleTopology>(std::move(et));
  }

  GraphTopologyTypes::AdjIndexVec out_indices;
  GraphTopologyTypes::EdgeDestVec out_dests;
  GraphTopologyTypes::PropIndexVec edge_prop_indices;
  GraphTopologyTypes::AdjIndexVec out_dests_offset;

  out_indices.allocateInterleaved(topology.num_nodes());
  out_dests.allocateInterleaved(topology.num_edges());
  edge_prop_indices.allocateInterleaved(topology.num_edges());
  out_dests_offset.allocateInterleaved(topology.num_nodes());

  katana::ParallelSTL::fill(out_indices.begin(), out_indices.end(), Edge{0});

  // Keep a copy of old destinaton ids and compute number of
  // in-coming edges for the new prefix sum of out_indices.
  katana::do_all(
      katana::iterate(topology.all_edges()),
      [&](Edge e) {
        // Counting outgoing edges in the tranpose graph by
        // counting incoming edges in the original graph
        auto dest = topology.edge_dest(e);
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
      katana::iterate(Edge{1}, Edge{topology.num_nodes()}),
      [&](Edge n) { out_dests_offset[n] = out_indices[n - 1]; },
      katana::no_stats());

  // Update out_dests with the new destination ids
  // of the transposed graphs
  katana::do_all(
      katana::iterate(topology.all_nodes()),
      [&](auto src) {
        // get all outgoing edges of a particular
        // node and reverse the edges.
        for (GraphTopology::Edge e : topology.edges(src)) {
          // e = start index into edge array for a particular node
          // Destination node
          auto dest = topology.edge_dest(e);
          // Location to save edge
          auto e_new = __sync_fetch_and_add(&(out_dests_offset[dest]), 1);
          // Save src as destination
          out_dests[e_new] = src;
          // remember the original edge ID to look up properties
          edge_prop_indices[e_new] = e;
        }
      },
      katana::steal(), katana::no_stats());

  return std::make_unique<EdgeShuffleTopology>(EdgeShuffleTopology{
      TransposeKind::kYes, EdgeSortKind::kAny, std::move(out_indices),
      std::move(out_dests), std::move(edge_prop_indices)});
}

std::unique_ptr<katana::EdgeShuffleTopology>
katana::EdgeShuffleTopology::MakeOriginalCopy(const katana::PropertyGraph* pg) {
  GraphTopology copy_topo = GraphTopology::Copy(pg->topology());

  GraphTopologyTypes::PropIndexVec edge_prop_indices;
  edge_prop_indices.allocateInterleaved(copy_topo.num_edges());
  katana::ParallelSTL::iota(
      edge_prop_indices.begin(), edge_prop_indices.end(), Edge{0});

  return std::make_unique<EdgeShuffleTopology>(EdgeShuffleTopology{
      TransposeKind::kNo, EdgeSortKind::kAny,
      std::move(copy_topo.GetAdjIndices()), std::move(copy_topo.GetDests()),
      std::move(edge_prop_indices)});
}

katana::GraphTopologyTypes::edge_iterator
katana::EdgeShuffleTopology::find_edge(
    const katana::GraphTopologyTypes::Node& src,
    const katana::GraphTopologyTypes::Node& dst) const noexcept {
  auto e_range = edges(src);

  constexpr size_t kBinarySearchThreshold = 64;

  if (e_range.size() > kBinarySearchThreshold &&
      !has_edges_sorted_by(EdgeSortKind::kSortedByDestID)) {
    KATANA_WARN_ONCE(
        "find_edge(): expect poor performance. Edges not sorted by Dest ID");
  }

  if (e_range.size() <= kBinarySearchThreshold) {
    auto iter = std::find_if(
        e_range.begin(), e_range.end(),
        [&](const GraphTopology::Edge& e) { return edge_dest(e) == dst; });

    return iter;

  } else {
    auto iter = std::lower_bound(
        e_range.begin(), e_range.end(), dst,
        internal::EdgeDestComparator<EdgeShuffleTopology>{this});

    return edge_dest(*iter) == dst ? iter : e_range.end();
  }
}

katana::GraphTopologyTypes::edges_range
katana::EdgeShuffleTopology::find_edges(
    const katana::GraphTopologyTypes::Node& src,
    const katana::GraphTopologyTypes::Node& dst) const noexcept {
  auto e_range = edges(src);
  if (e_range.empty()) {
    return e_range;
  }

  KATANA_LOG_VASSERT(
      !has_edges_sorted_by(EdgeSortKind::kSortedByDestID),
      "Must have edges sorted by kSortedByDestID");

  internal::EdgeDestComparator<EdgeShuffleTopology> comp{this};
  auto [first_it, last_it] =
      std::equal_range(e_range.begin(), e_range.end(), dst, comp);

  if (first_it == e_range.end() || edge_dest(*first_it) != dst) {
    // return empty range
    return MakeStandardRange(e_range.end(), e_range.end());
  }

  auto ret_range = MakeStandardRange(first_it, last_it);
  for ([[maybe_unused]] auto e : ret_range) {
    KATANA_LOG_DEBUG_ASSERT(edge_dest(e) == dst);
  }
  return ret_range;
}

void
katana::EdgeShuffleTopology::SortEdgesByDestID() noexcept {
  katana::do_all(
      katana::iterate(Base::all_nodes()),
      [&](Node node) {
        // get this node's first and last edge
        auto e_beg = *Base::edges(node).begin();
        auto e_end = *Base::edges(node).end();

        // get iterators to locations to sort in the vector
        auto begin_sort_iter = katana::make_zip_iterator(
            edge_prop_indices_.begin() + e_beg,
            Base::GetDests().begin() + e_beg);

        auto end_sort_iter = katana::make_zip_iterator(
            edge_prop_indices_.begin() + e_end,
            Base::GetDests().begin() + e_end);

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
      },
      katana::steal(), katana::no_stats());
  // remember to update sort state
  edge_sort_state_ = EdgeSortKind::kSortedByDestID;
}

void
katana::EdgeShuffleTopology::SortEdgesByTypeThenDest(
    const PropertyGraph* pg) noexcept {
  katana::do_all(
      katana::iterate(Base::all_nodes()),
      [&](Node node) {
        // get this node's first and last edge
        auto e_beg = *Base::edges(node).begin();
        auto e_end = *Base::edges(node).end();

        // get iterators to locations to sort in the vector
        auto begin_sort_iter = katana::make_zip_iterator(
            edge_prop_indices_.begin() + e_beg,
            Base::GetDests().begin() + e_beg);

        auto end_sort_iter = katana::make_zip_iterator(
            edge_prop_indices_.begin() + e_end,
            Base::GetDests().begin() + e_end);

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

              EntityType data1 = pg->GetTypeOfEdge(e1);
              EntityType data2 = pg->GetTypeOfEdge(e2);
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
  edge_sort_state_ = EdgeSortKind::kSortedByEdgeType;
}

void
katana::EdgeShuffleTopology::SortEdgesByDestType(
    const PropertyGraph*,
    const katana::GraphTopologyTypes::PropIndexVec&) noexcept {
  KATANA_LOG_FATAL("Not implemented yet");
}

std::unique_ptr<katana::ShuffleTopology>
katana::ShuffleTopology::MakeSortedByDegree(
    const PropertyGraph*,
    const katana::EdgeShuffleTopology& seed_topo) noexcept {
  auto cmp = [&](const auto& i1, const auto& i2) {
    auto d1 = seed_topo.degree(i1);
    auto d2 = seed_topo.degree(i2);
    if (d1 == d2) {
      return i1 < i2;
    }
    return d1 < d2;
  };

  return MakeNodeSortedTopo(seed_topo, cmp, NodeSortKind::kSortedByDegree);
}

std::unique_ptr<katana::ShuffleTopology>
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

  return MakeNodeSortedTopo(seed_topo, cmp, NodeSortKind::kSortedByNodeType);
}

std::unique_ptr<katana::CondensedTypeIDMap>
katana::CondensedTypeIDMap::MakeFromEdgeTypes(
    const katana::PropertyGraph* pg) noexcept {
  TypeIDToIndexMap edge_type_to_index;
  IndexToTypeIDMap edge_index_to_type;

  katana::PerThreadStorage<katana::gstl::Set<EntityType>> edgeTypes;

  const auto& topo = pg->topology();

  katana::do_all(
      katana::iterate(Edge{0}, topo.num_edges()),
      [&](const Edge& e) {
        EntityType type = pg->GetTypeOfEdge(e);
        edgeTypes.getLocal()->insert(type);
      },
      katana::no_stats());

  // ordered map
  std::set<EntityType> mergedSet;
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
    *edgeTypes.getLocal() = gstl::Set<EntityType>();
  });

  return std::make_unique<CondensedTypeIDMap>(CondensedTypeIDMap{
      std::move(edge_type_to_index), std::move(edge_index_to_type)});
}

katana::EdgeTypeAwareTopology::AdjIndexVec
katana::EdgeTypeAwareTopology::CreatePerEdgeTypeAdjacencyIndex(
    const PropertyGraph* pg, const CondensedTypeIDMap* edge_type_index,
    const EdgeShuffleTopology* e_topo) noexcept {
  if (e_topo->num_nodes() == 0) {
    KATANA_LOG_VASSERT(
        e_topo->num_edges() == 0, "Found graph with edges but no nodes");
    return AdjIndexVec{};
  }

  if (edge_type_index->num_unique_types() == 0) {
    KATANA_LOG_VASSERT(
        e_topo->num_edges() == 0, "Found graph with edges but no edge types");
    // Graph has some nodes but no edges.
    return AdjIndexVec{};
  }

  const size_t sz = e_topo->num_nodes() * edge_type_index->num_unique_types();
  AdjIndexVec adj_indices;
  adj_indices.allocateInterleaved(sz);

  katana::do_all(
      katana::iterate(e_topo->all_nodes()),
      [&](Node N) {
        auto offset = N * edge_type_index->num_unique_types();
        uint32_t index = 0;
        for (auto e : e_topo->edges(N)) {
          // Since we sort the edges, we must use the
          // edge_property_index because EdgeShuffleTopology rearranges the edges
          const auto type = pg->GetTypeOfEdge(e_topo->edge_property_index(e));
          while (type != edge_type_index->GetType(index)) {
            adj_indices[offset + index] = e;
            index++;
            KATANA_LOG_DEBUG_ASSERT(
                index < edge_type_index->num_unique_types());
          }
        }
        auto e = *e_topo->edges(N).end();
        while (index < edge_type_index->num_unique_types()) {
          adj_indices[offset + index] = e;
          index++;
        }
      },
      katana::no_stats(), katana::steal());

  return adj_indices;
}

std::unique_ptr<katana::EdgeTypeAwareTopology>
katana::EdgeTypeAwareTopology::MakeFrom(
    const katana::PropertyGraph* pg,
    const katana::CondensedTypeIDMap* edge_type_index,
    const katana::EdgeShuffleTopology* e_topo) noexcept {
  KATANA_LOG_DEBUG_ASSERT(e_topo->has_edges_sorted_by(
      EdgeShuffleTopology::EdgeSortKind::kSortedByEdgeType));

  KATANA_LOG_DEBUG_ASSERT(e_topo->num_edges() == pg->topology().num_edges());

  AdjIndexVec per_type_adj_indices =
      CreatePerEdgeTypeAdjacencyIndex(pg, edge_type_index, e_topo);

  return std::make_unique<EdgeTypeAwareTopology>(EdgeTypeAwareTopology{
      pg, edge_type_index, e_topo, std::move(per_type_adj_indices)});
}

std::unique_ptr<katana::ProjectedTopology>
katana::ProjectedTopology::CreateEmptyEdgeProjectedTopology(
    const katana::PropertyGraph* pg, uint32_t num_new_nodes) {
  const auto& topology = pg->topology();

  katana::NUMAArray<Edge> out_indices;
  out_indices.allocateInterleaved(num_new_nodes);

  katana::NUMAArray<Node> out_dests;
  katana::NUMAArray<Node> original_to_projected_nodes_mapping;
  original_to_projected_nodes_mapping.allocateInterleaved(topology.num_nodes());
  katana::ParallelSTL::fill(
      original_to_projected_nodes_mapping.begin(),
      original_to_projected_nodes_mapping.end(),
      static_cast<Node>(topology.num_nodes()));

  katana::NUMAArray<Node> projected_to_original_nodes_mapping;
  projected_to_original_nodes_mapping.allocateInterleaved(num_new_nodes);

  katana::NUMAArray<Edge> original_to_projected_edges_mapping;
  katana::NUMAArray<Edge> projected_to_original_edges_mapping;

  original_to_projected_edges_mapping.allocateInterleaved(topology.num_edges());
  katana::ParallelSTL::fill(
      original_to_projected_edges_mapping.begin(),
      original_to_projected_edges_mapping.end(), Edge{topology.num_edges()});

  return std::make_unique<katana::ProjectedTopology>(katana::ProjectedTopology{
      std::move(out_indices), std::move(out_dests),
      std::move(original_to_projected_nodes_mapping),
      std::move(projected_to_original_nodes_mapping),
      std::move(original_to_projected_edges_mapping),
      std::move(projected_to_original_edges_mapping)});
}

std::unique_ptr<katana::ProjectedTopology>
katana::ProjectedTopology::CreateEmptyProjectedTopology(
    const katana::PropertyGraph* pg) {
  return CreateEmptyEdgeProjectedTopology(pg, 0);
}

std::unique_ptr<katana::ProjectedTopology>
katana::ProjectedTopology::MakeTypeProjectedTopology(
    const katana::PropertyGraph* pg, const std::vector<std::string>& node_types,
    const std::vector<std::string>& edge_types) {
  KATANA_LOG_DEBUG_ASSERT(pg);

  const auto& topology = pg->topology();
  if (topology.empty()) {
    return std::make_unique<ProjectedTopology>(ProjectedTopology());
  }

  // calculate number of new nodes
  uint32_t num_new_nodes = 0;
  uint32_t num_new_edges = 0;

  katana::DynamicBitset bitset_nodes;
  bitset_nodes.resize(topology.num_nodes());

  NUMAArray<Node> original_to_projected_nodes_mapping;
  original_to_projected_nodes_mapping.allocateInterleaved(topology.num_nodes());

  if (node_types.empty()) {
    num_new_nodes = topology.num_nodes();
    // set all nodes
    katana::do_all(katana::iterate(topology.all_nodes()), [&](auto src) {
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
    katana::GAccumulator<uint64_t> accum_num_new_edges;

    katana::do_all(katana::iterate(topology.all_nodes()), [&](auto src) {
      for (auto type : node_entity_type_ids) {
        if (pg->DoesNodeHaveType(src, type)) {
          accum_num_new_nodes += 1;
          auto it_begin = topology.edges(src).begin();
          auto it_end = topology.edges(src).end();

          accum_num_new_edges += it_end - it_begin;
          bitset_nodes.set(src);
          // this sets the correspondign entry in the array to 1
          // will perform a prefix sum on this array later on
          original_to_projected_nodes_mapping[src] = 1;
          return;
        }
      }
    });
    num_new_nodes = accum_num_new_nodes.reduce();
    num_new_edges = accum_num_new_edges.reduce();

    if (num_new_nodes == 0) {
      // no nodes selected;
      // return empty graph
      return CreateEmptyProjectedTopology(pg);
    }
  }

  // fill old to new nodes mapping
  katana::ParallelSTL::partial_sum(
      original_to_projected_nodes_mapping.begin(),
      original_to_projected_nodes_mapping.end(),
      original_to_projected_nodes_mapping.begin());

  NUMAArray<Node> projected_to_original_nodes_mapping;
  projected_to_original_nodes_mapping.allocateInterleaved(num_new_nodes);

  katana::do_all(katana::iterate(topology.all_nodes()), [&](auto src) {
    if (bitset_nodes.test(src)) {
      original_to_projected_nodes_mapping[src]--;
      projected_to_original_nodes_mapping
          [original_to_projected_nodes_mapping[src]] = src;
    } else {
      original_to_projected_nodes_mapping[src] = topology.num_nodes();
    }
  });

  // calculate number of new edges
  katana::DynamicBitset bitset_edges;
  bitset_edges.resize(topology.num_edges());

  NUMAArray<Edge> out_indices;
  out_indices.allocateInterleaved(num_new_nodes);

  // initializes the edge-index array to all zeros
  katana::ParallelSTL::fill(out_indices.begin(), out_indices.end(), Edge{0});

  if (edge_types.empty()) {
    // set all edges incident to projected nodes
    katana::do_all(
        katana::iterate(Node{0}, Node{num_new_nodes}),
        [&](auto src) {
          auto old_src = projected_to_original_nodes_mapping[src];
          for (Edge e : topology.edges(old_src)) {
            auto dest = topology.edge_dest(e);
            if (bitset_nodes.test(dest)) {
              bitset_edges.set(e);
              out_indices[src] += 1;
            }
          }
        },
        katana::steal());
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

          for (Edge e : topology.edges(old_src)) {
            auto dest = topology.edge_dest(e);
            if (bitset_nodes.test(dest)) {
              for (auto type : edge_entity_type_ids) {
                if (pg->DoesEdgeHaveType(e, type)) {
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
      return CreateEmptyEdgeProjectedTopology(pg, num_new_nodes);
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

  out_dests.allocateInterleaved(num_new_edges);
  original_to_projected_edges_mapping.allocateInterleaved(topology.num_edges());
  projected_to_original_edges_mapping.allocateInterleaved(num_new_edges);

  // Update out_dests with the new destination ids
  katana::do_all(
      katana::iterate(Node{0}, Node{num_new_nodes}),
      [&](Node n) {
        auto src = projected_to_original_nodes_mapping[n];

        for (Edge e : topology.edges(src)) {
          if (bitset_edges.test(e)) {
            auto e_new = out_dests_offset[n];
            out_dests_offset[n]++;

            auto dest = topology.edge_dest(e);
            dest = original_to_projected_nodes_mapping[dest];
            out_dests[e_new] = dest;

            original_to_projected_edges_mapping[e] = e_new;
            projected_to_original_edges_mapping[e_new] = e;
          }
        }
      },
      katana::steal());

  katana::do_all(katana::iterate(topology.all_edges()), [&](auto edge) {
    if (!bitset_edges.test(edge)) {
      original_to_projected_edges_mapping[edge] = topology.num_edges();
    }
  });

  return std::make_unique<ProjectedTopology>(ProjectedTopology{
      std::move(out_indices), std::move(out_dests),
      std::move(original_to_projected_nodes_mapping),
      std::move(projected_to_original_nodes_mapping),
      std::move(original_to_projected_edges_mapping),
      std::move(projected_to_original_edges_mapping)});
}
const katana::GraphTopology*
katana::PGViewCache::GetOriginalTopology(
    const PropertyGraph* pg) const noexcept {
  return &pg->topology();
}

katana::CondensedTypeIDMap*
katana::PGViewCache::BuildOrGetEdgeTypeIndex(
    const katana::PropertyGraph* pg) noexcept {
  if (edge_type_id_map_ && edge_type_id_map_->is_valid()) {
    return edge_type_id_map_.get();
  }

  edge_type_id_map_ = CondensedTypeIDMap::MakeFromEdgeTypes(pg);
  KATANA_LOG_DEBUG_ASSERT(edge_type_id_map_);
  return edge_type_id_map_.get();
};

template <typename Topo>
[[maybe_unused]] bool
CheckTopology(const katana::PropertyGraph* pg, const Topo* t) noexcept {
  return (pg->num_nodes() == t->num_nodes()) &&
         (pg->num_edges() == t->num_edges());
}

katana::EdgeShuffleTopology*
katana::PGViewCache::BuildOrGetEdgeShuffTopo(
    const katana::PropertyGraph* pg,
    const katana::EdgeShuffleTopology::TransposeKind& tpose_kind,
    const katana::EdgeShuffleTopology::EdgeSortKind& sort_kind) noexcept {
  auto pred = [&](const auto& topo_ptr) {
    return topo_ptr->is_valid() && topo_ptr->has_transpose_state(tpose_kind) &&
           topo_ptr->has_edges_sorted_by(sort_kind);
  };
  auto it =
      std::find_if(edge_shuff_topos_.begin(), edge_shuff_topos_.end(), pred);

  if (it != edge_shuff_topos_.end()) {
    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, it->get()));
    return it->get();
  } else {
    edge_shuff_topos_.emplace_back(
        EdgeShuffleTopology::Make(pg, tpose_kind, sort_kind));
    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, edge_shuff_topos_.back().get()));
    return edge_shuff_topos_.back().get();
  }
}

katana::ShuffleTopology*
katana::PGViewCache::BuildOrGetShuffTopo(
    const katana::PropertyGraph* pg,
    const katana::EdgeShuffleTopology::TransposeKind& tpose_kind,
    const katana::ShuffleTopology::NodeSortKind& node_sort_todo,
    const katana::EdgeShuffleTopology::EdgeSortKind& edge_sort_todo) noexcept {
  auto pred = [&](const auto& topo_ptr) {
    return topo_ptr->is_valid() && topo_ptr->has_transpose_state(tpose_kind) &&
           topo_ptr->has_edges_sorted_by(edge_sort_todo) &&
           topo_ptr->has_nodes_sorted_by(node_sort_todo);
  };

  auto it =
      std::find_if(fully_shuff_topos_.begin(), fully_shuff_topos_.end(), pred);

  if (it != fully_shuff_topos_.end()) {
    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, it->get()));
    return it->get();
  } else {
    // EdgeShuffleTopology e_topo below is going to serve as a seed for
    // ShuffleTopology, so we only care about transpose state, and not the sort
    // state. Because, when creating ShuffleTopology, once we shuffle the nodes, we
    // will need to re-sort the edges even if they were already sorted
    auto e_topo = BuildOrGetEdgeShuffTopo(pg, tpose_kind, EdgeShuffleTopology::EdgeSortKind::kAny);
    KATANA_LOG_DEBUG_ASSERT(e_topo->has_transpose_state(tpose_kind));

    fully_shuff_topos_.emplace_back(ShuffleTopology::MakeFromTopo(
        pg, *e_topo, node_sort_todo, edge_sort_todo));

    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, fully_shuff_topos_.back().get()));
    return fully_shuff_topos_.back().get();
  }
}

katana::EdgeTypeAwareTopology*
katana::PGViewCache::BuildOrGetEdgeTypeAwareTopo(
    const katana::PropertyGraph* pg,
    const katana::EdgeShuffleTopology::TransposeKind& tpose_kind) noexcept {
  auto pred = [&](const auto& topo_ptr) {
    return topo_ptr->is_valid() && topo_ptr->has_transpose_state(tpose_kind);
  };
  auto it = std::find_if(
      edge_type_aware_topos_.begin(), edge_type_aware_topos_.end(), pred);

  if (it != edge_type_aware_topos_.end()) {
    KATANA_LOG_DEBUG_ASSERT(CheckTopology(pg, it->get()));
    return it->get();
  } else {
    auto sorted_topo = BuildOrGetEdgeShuffTopo(
        pg, tpose_kind, EdgeShuffleTopology::EdgeSortKind::kSortedByEdgeType);
    auto edge_type_index = BuildOrGetEdgeTypeIndex(pg);
    edge_type_aware_topos_.emplace_back(
        EdgeTypeAwareTopology::MakeFrom(pg, edge_type_index, sorted_topo));

    KATANA_LOG_DEBUG_ASSERT(
        CheckTopology(pg, edge_type_aware_topos_.back().get()));
    return edge_type_aware_topos_.back().get();
  }
}

katana::ProjectedTopology*
katana::PGViewCache::BuildOrGetProjectedGraphTopo(
    const PropertyGraph* pg, const std::vector<std::string>& node_types,
    const std::vector<std::string>& edge_types) noexcept {
  if (projected_topos_) {
    return projected_topos_.get();
  }

  projected_topos_ =
      ProjectedTopology::MakeTypeProjectedTopology(pg, node_types, edge_types);
  KATANA_LOG_DEBUG_ASSERT(projected_topos_);
  return projected_topos_.get();
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

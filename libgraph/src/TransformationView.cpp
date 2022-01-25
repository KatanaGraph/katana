#include "katana/TransformationView.h"

namespace {
/// This function converts a bitset to a bitmask
void
FillBitMask(
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
}  // namespace

namespace katana {
std::unique_ptr<TransformationView>
TransformationView::CreateEmptyEdgeProjectedTopology(
    const PropertyGraph& pg, uint32_t num_new_nodes,
    const DynamicBitset& bitset) {
  const auto& topology = pg.topology();

  NUMAArray<Edge> out_indices;
  out_indices.allocateInterleaved(num_new_nodes);

  NUMAArray<Node> out_dests;
  NUMAArray<Node> original_to_projected_nodes_mapping;
  original_to_projected_nodes_mapping.allocateInterleaved(topology.NumNodes());
  katana::ParallelSTL::fill(
      original_to_projected_nodes_mapping.begin(),
      original_to_projected_nodes_mapping.end(),
      static_cast<Node>(topology.NumNodes()));

  NUMAArray<Node> projected_to_original_nodes_mapping;
  projected_to_original_nodes_mapping.allocateInterleaved(num_new_nodes);

  NUMAArray<Edge> original_to_projected_edges_mapping;
  NUMAArray<Edge> projected_to_original_edges_mapping;

  original_to_projected_edges_mapping.allocateInterleaved(topology.NumEdges());
  katana::ParallelSTL::fill(
      original_to_projected_edges_mapping.begin(),
      original_to_projected_edges_mapping.end(), Edge{topology.NumEdges()});

  NUMAArray<uint8_t> node_bitmask;
  node_bitmask.allocateInterleaved((topology.NumNodes() + 7) / 8);

  FillBitMask(topology.NumNodes(), bitset, &node_bitmask);

  NUMAArray<uint8_t> edge_bitmask;
  edge_bitmask.allocateInterleaved((topology.NumEdges() + 7) / 8);

  GraphTopology topo{std::move(out_indices), std::move(out_dests)};

  PropertyGraph::Transformation transformation{
      std::move(original_to_projected_nodes_mapping),
      std::move(projected_to_original_nodes_mapping),
      std::move(original_to_projected_edges_mapping),
      std::move(projected_to_original_edges_mapping),
      std::move(node_bitmask),
      std::move(edge_bitmask)};

  // Using `new` to access a non-public constructor.
  return std::unique_ptr<katana::TransformationView>(
      new katana::TransformationView(
          pg, std::move(topo), std::move(transformation)));
}

std::unique_ptr<TransformationView>
katana::TransformationView::CreateEmptyProjectedTopology(
    const katana::PropertyGraph& pg, const katana::DynamicBitset& bitset) {
  return CreateEmptyEdgeProjectedTopology(pg, 0, bitset);
}

std::unique_ptr<TransformationView>
katana::TransformationView::MakeProjectedGraph(
    const PropertyGraph& pg, const std::vector<std::string>& node_types,
    const std::vector<std::string>& edge_types) {
  const auto& topology = pg.topology();
  if (topology.empty()) {
    return std::make_unique<TransformationView>();
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
      auto entity_type_id = pg.GetNodeEntityTypeID(node_type);
      node_entity_type_ids.insert(entity_type_id);
    }

    katana::GAccumulator<uint32_t> accum_num_new_nodes;

    katana::do_all(katana::iterate(topology.Nodes()), [&](auto src) {
      for (auto type : node_entity_type_ids) {
        if (pg.DoesNodeHaveType(src, type)) {
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
      auto entity_type_id = pg.GetEdgeEntityTypeID(edge_type);
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
                if (pg.DoesEdgeHaveTypeFromTopoIndex(e, type)) {
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

  GraphTopology topo{std::move(out_indices), std::move(out_dests)};

  Transformation transformation{
      std::move(original_to_projected_nodes_mapping),
      std::move(projected_to_original_nodes_mapping),
      std::move(original_to_projected_edges_mapping),
      std::move(projected_to_original_edges_mapping),
      std::move(node_bitmask),
      std::move(edge_bitmask)};

  // Using `new` to access a non-public constructor.
  return std::unique_ptr<TransformationView>(
      new TransformationView(pg, std::move(topo), std::move(transformation)));
}

Result<void>
TransformationView::AddNodeProperties(
    const std::shared_ptr<arrow::Table>& props, TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("adding empty node prop table");
    return ResultSuccess();
  }
  if (NumOriginalNodes() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        NumOriginalNodes(), props->num_rows());
  }
  return rdg().AddNodeProperties(props, txn_ctx);
}

Result<void>
TransformationView::UpsertNodeProperties(
    const std::shared_ptr<arrow::Table>& props, TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("upsert empty node prop table");
    return ResultSuccess();
  }
  if (NumOriginalNodes() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        NumOriginalNodes(), props->num_rows());
  }
  return rdg().UpsertNodeProperties(props, txn_ctx);
}

Result<void>
TransformationView::AddEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("adding empty edge prop table");
    return ResultSuccess();
  }
  if (NumOriginalEdges() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().NumEdges(), props->num_rows());
  }
  return rdg().AddEdgeProperties(props, txn_ctx);
}

Result<void>
TransformationView::UpsertEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("upsert empty edge prop table");
    return ResultSuccess();
  }
  if (NumOriginalEdges() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().NumEdges(), props->num_rows());
  }
  return rdg().UpsertEdgeProperties(props, txn_ctx);
}

}  // namespace katana

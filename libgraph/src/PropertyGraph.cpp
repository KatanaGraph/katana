#include "katana/PropertyGraph.h"

#include <stdio.h>
#include <sys/mman.h>

#include <memory>
#include <utility>
#include <vector>

#include <arrow/array.h>

#include "katana/ArrowInterchange.h"
#include "katana/ErrorCode.h"
#include "katana/FileFrame.h"
#include "katana/GraphTopology.h"
#include "katana/Iterators.h"
#include "katana/Logging.h"
#include "katana/Loops.h"
#include "katana/NUMAArray.h"
#include "katana/PerThreadStorage.h"
#include "katana/Platform.h"
#include "katana/Properties.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/RDGPrefix.h"
#include "katana/RDGStorageFormatVersion.h"
#include "katana/RDGTopology.h"
#include "katana/Result.h"
#include "katana/tsuba.h"

namespace {

[[maybe_unused]] bool
CheckTopology(
    const uint64_t* out_indices, const uint64_t num_nodes,
    const uint32_t* out_dests, const uint64_t num_edges) {
  bool has_bad_adj = false;

  katana::do_all(
      katana::iterate(uint64_t{0}, num_nodes),
      [&](auto n) {
        if (out_indices[n] > num_edges) {
          has_bad_adj = true;
        }
      },
      katana::no_stats());

  bool has_bad_dest = false;
  katana::do_all(
      katana::iterate(uint64_t{0}, num_edges),
      [&](auto e) {
        if (out_dests[e] >= num_nodes) {
          has_bad_dest = true;
        }
      },
      katana::no_stats());

  return !has_bad_adj && !has_bad_dest;
}

/// MapEntityTypeIDsFromFile takes a file buffer of a node or edge Type set ID file
/// and extracts the property graph type set ids from it. It is an alternative way
/// of extracting EntityTypeIDs and extraction from properties will be depreciated in
/// favor of this method.
katana::Result<katana::PropertyGraph::EntityTypeIDArray>
MapEntityTypeIDsArray(
    const katana::FileView& file_view, size_t num_entries,
    bool is_headerless_entity_type_id_array) {
  // allocate type IDs array
  katana::PropertyGraph::EntityTypeIDArray entity_type_id_array;
  entity_type_id_array.allocateInterleaved(num_entries);

  const katana::EntityTypeID* type_IDs_array = nullptr;

  if (is_headerless_entity_type_id_array) {
    type_IDs_array = file_view.ptr<katana::EntityTypeID>();
  } else {
    // If we have header, the file_view should not be empty
    if (file_view.size() == 0) {
      return katana::ErrorCode::InvalidArgument;
    }

    const auto* data = file_view.ptr<katana::EntityTypeIDArrayHeader>();
    type_IDs_array = reinterpret_cast<const katana::EntityTypeID*>(&data[1]);
  }

  if (num_entries != 0) {
    KATANA_LOG_DEBUG_ASSERT(type_IDs_array != nullptr);
  }

  katana::ParallelSTL::copy(
      &type_IDs_array[0], &type_IDs_array[num_entries],
      entity_type_id_array.begin());

  return katana::MakeResult(std::move(entity_type_id_array));
}

katana::Result<std::unique_ptr<katana::FileFrame>>
WriteEntityTypeIDsArray(
    const katana::NUMAArray<katana::EntityTypeID>& entity_type_id_array) {
  auto ff = std::make_unique<katana::FileFrame>();

  KATANA_CHECKED(ff->Init());

  if (entity_type_id_array.size()) {
    const katana::EntityTypeID* raw = entity_type_id_array.data();
    auto buf = arrow::Buffer::Wrap(raw, entity_type_id_array.size());
    arrow::Status aro_sts = ff->Write(buf);
    if (!aro_sts.ok()) {
      return katana::ArrowToKatana(aro_sts.code());
    }
  }

  return std::unique_ptr<katana::FileFrame>(std::move(ff));
}

katana::PropertyGraph::EntityTypeIDArray
MakeDefaultEntityTypeIDArray(size_t vec_sz) {
  katana::PropertyGraph::EntityTypeIDArray type_ids;
  type_ids.allocateInterleaved(vec_sz);
  katana::ParallelSTL::fill(
      type_ids.begin(), type_ids.end(), katana::kUnknownEntityType);
  return type_ids;
}

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

katana::PropertyGraph::~PropertyGraph() = default;

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    std::unique_ptr<katana::RDGFile> rdg_file, katana::RDG&& rdg,
    katana::TxnContext* txn_ctx) {
  // find & map the default csr topology
  katana::RDGTopology shadow_csr = katana::RDGTopology::MakeShadowCSR();
  katana::RDGTopology* csr = KATANA_CHECKED_CONTEXT(
      rdg.GetTopology(shadow_csr),
      "unable to find csr topology, must have csr topology to Make a "
      "PropertyGraph");

  KATANA_LOG_DEBUG_ASSERT(CheckTopology(
      csr->adj_indices(), csr->num_nodes(), csr->dests(), csr->num_edges()));
  katana::GraphTopology topo = katana::GraphTopology(
      csr->adj_indices(), csr->num_nodes(), csr->dests(), csr->num_edges());

  // The GraphTopology constructor copies all of the required topology data.
  // Clean up the RDGTopologies memory
  KATANA_CHECKED(csr->unbind_file_storage());

  if (rdg.IsEntityTypeIDsOutsideProperties()) {
    KATANA_LOG_DEBUG("loading EntityType data from outside properties");

    EntityTypeIDArray node_type_ids = KATANA_CHECKED(MapEntityTypeIDsArray(
        rdg.node_entity_type_id_array_file_storage(), topo.NumNodes(),
        rdg.IsHeaderlessEntityTypeIDArray()));

    EntityTypeIDArray edge_type_ids = KATANA_CHECKED(MapEntityTypeIDsArray(
        rdg.edge_entity_type_id_array_file_storage(), topo.NumEdges(),
        rdg.IsHeaderlessEntityTypeIDArray()));

    KATANA_ASSERT(topo.NumNodes() == node_type_ids.size());
    KATANA_ASSERT(topo.NumEdges() == edge_type_ids.size());

    EntityTypeManager node_type_manager =
        KATANA_CHECKED(rdg.node_entity_type_manager());
    EntityTypeManager edge_type_manager =
        KATANA_CHECKED(rdg.edge_entity_type_manager());

    return std::make_unique<PropertyGraph>(
        std::move(rdg_file), std::move(rdg), std::move(topo),
        std::move(node_type_ids), std::move(edge_type_ids),
        std::move(node_type_manager), std::move(edge_type_manager));
  } else {
    // we must construct id_arrays and managers from properties

    auto pg = std::make_unique<PropertyGraph>(
        std::move(rdg_file), std::move(rdg), std::move(topo),
        MakeDefaultEntityTypeIDArray(topo.NumNodes()),
        MakeDefaultEntityTypeIDArray(topo.NumEdges()), EntityTypeManager{},
        EntityTypeManager{});

    KATANA_CHECKED(pg->ConstructEntityTypeIDs(txn_ctx));

    return MakeResult(std::move(pg));
  }
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    const katana::URI& rdg_dir, katana::TxnContext* txn_ctx,
    const katana::RDGLoadOptions& opts) {
  katana::RDGManifest manifest =
      KATANA_CHECKED(katana::FindManifest(rdg_dir, txn_ctx));
  auto rdg_handle =
      KATANA_CHECKED(katana::Open(std::move(manifest), katana::kReadWrite));
  auto new_file = std::make_unique<katana::RDGFile>(rdg_handle);

  return Make(std::move(new_file), txn_ctx, opts);
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    std::unique_ptr<RDGFile> rdg_file, katana::TxnContext* txn_ctx,
    const katana::RDGLoadOptions& opts) {
  auto rdg = KATANA_CHECKED(RDG::Make(*rdg_file, opts));
  return katana::PropertyGraph::Make(
      std::move(rdg_file), std::move(rdg), txn_ctx);
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(katana::GraphTopology&& topo_to_assign) {
  return std::make_unique<katana::PropertyGraph>(
      std::unique_ptr<katana::RDGFile>(), katana::RDG{},
      std::move(topo_to_assign),
      MakeDefaultEntityTypeIDArray(topo_to_assign.NumNodes()),
      MakeDefaultEntityTypeIDArray(topo_to_assign.NumEdges()),
      EntityTypeManager{}, EntityTypeManager{});
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    const katana::URI& rdg_dir, katana::GraphTopology&& topo_to_assign) {
  return Make(
      rdg_dir, std::move(topo_to_assign),
      MakeDefaultEntityTypeIDArray(topo_to_assign.NumNodes()),
      MakeDefaultEntityTypeIDArray(topo_to_assign.NumEdges()),
      EntityTypeManager{}, EntityTypeManager{});
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    katana::GraphTopology&& topo_to_assign,
    NUMAArray<EntityTypeID>&& node_entity_type_ids,
    NUMAArray<EntityTypeID>&& edge_entity_type_ids,
    EntityTypeManager&& node_type_manager,
    EntityTypeManager&& edge_type_manager) {
  return std::make_unique<katana::PropertyGraph>(
      std::unique_ptr<katana::RDGFile>(), katana::RDG{},
      std::move(topo_to_assign), std::move(node_entity_type_ids),
      std::move(edge_entity_type_ids), std::move(node_type_manager),
      std::move(edge_type_manager));
}

std::unique_ptr<katana::PropertyGraph>
katana::PropertyGraph::MakeEmptyEdgeProjectedGraph(
    PropertyGraph& pg, uint32_t num_new_nodes,
    const DynamicBitset& nodes_bitset,
    NUMAArray<Node>&& original_to_projected_nodes_mapping,
    NUMAArray<GraphTopology::PropertyIndex>&&
        projected_to_original_nodes_mapping) {
  const auto& topology = pg.topology();

  NUMAArray<Edge> out_indices;
  out_indices.allocateInterleaved(num_new_nodes);

  NUMAArray<Node> out_dests;
  NUMAArray<Edge> original_to_projected_edges_mapping;
  NUMAArray<GraphTopology::PropertyIndex> projected_to_original_edges_mapping;

  original_to_projected_edges_mapping.allocateInterleaved(topology.NumEdges());
  katana::ParallelSTL::fill(
      original_to_projected_edges_mapping.begin(),
      original_to_projected_edges_mapping.end(), Edge{topology.NumEdges()});

  NUMAArray<uint8_t> node_bitmask;
  node_bitmask.allocateInterleaved((topology.NumNodes() + 7) / 8);

  FillBitMask(topology.NumNodes(), nodes_bitset, &node_bitmask);

  NUMAArray<uint8_t> edge_bitmask;
  edge_bitmask.allocateInterleaved((topology.NumEdges() + 7) / 8);

  GraphTopology topo{
      std::move(out_indices), std::move(out_dests),
      std::move(projected_to_original_edges_mapping),
      std::move(projected_to_original_nodes_mapping)};

  // Using `new` to access a non-public constructor.
  return std::unique_ptr<katana::PropertyGraph>(new katana::PropertyGraph(
      pg, std::move(topo), std::move(original_to_projected_nodes_mapping),
      std::move(original_to_projected_edges_mapping), std::move(node_bitmask),
      std::move(edge_bitmask)));
}

std::unique_ptr<katana::PropertyGraph>
katana::PropertyGraph::MakeEmptyProjectedGraph(
    katana::PropertyGraph& pg, const katana::DynamicBitset& nodes_bitset) {
  const auto& topology = pg.topology();
  NUMAArray<Node> original_to_projected_nodes_mapping;
  original_to_projected_nodes_mapping.allocateInterleaved(topology.NumNodes());
  katana::ParallelSTL::fill(
      original_to_projected_nodes_mapping.begin(),
      original_to_projected_nodes_mapping.end(),
      static_cast<Node>(topology.NumNodes()));

  return MakeEmptyEdgeProjectedGraph(
      pg, 0, nodes_bitset, std::move(original_to_projected_nodes_mapping), {});
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    const katana::URI& rdg_dir, katana::GraphTopology&& topo_to_assign,
    NUMAArray<EntityTypeID>&& node_entity_type_ids,
    NUMAArray<EntityTypeID>&& edge_entity_type_ids,
    EntityTypeManager&& node_type_manager,
    EntityTypeManager&& edge_type_manager) {
  auto retval = std::make_unique<katana::PropertyGraph>(
      std::unique_ptr<katana::RDGFile>(), katana::RDG{},
      std::move(topo_to_assign), std::move(node_entity_type_ids),
      std::move(edge_entity_type_ids), std::move(node_type_manager),
      std::move(edge_type_manager));
  // It doesn't make sense to pass a RDGFile to the constructor because this
  // PropertyGraph wasn't loaded from a file. But all PropertyGraphs have an
  // associated storage location, so set one here.
  retval->rdg().set_rdg_dir(rdg_dir);
  return retval;
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Copy(katana::TxnContext* txn_ctx) const {
  return Copy(
      loaded_node_schema()->field_names(), loaded_edge_schema()->field_names(),
      txn_ctx);
}

std::unique_ptr<katana::PropertyGraph>
katana::PropertyGraph::MakeProjectedGraph(
    PropertyGraph& pg, const std::vector<std::string>& node_types,
    const std::vector<std::string>& edge_types) {
  auto ret = MakeProjectedGraph(
      pg, node_types.empty() ? std::nullopt : std::make_optional(node_types),
      edge_types.empty() ? std::nullopt : std::make_optional(edge_types));
  KATANA_LOG_VASSERT(ret.has_value(), "{}", ret.error());
  return std::move(ret.value());
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::MakeProjectedGraph(
    PropertyGraph& pg, std::optional<std::vector<std::string>> node_types,
    std::optional<std::vector<std::string>> edge_types) {
  std::optional<SetOfEntityTypeIDs> node_type_ids;
  if (node_types) {
    node_type_ids = KATANA_CHECKED(
        pg.GetNodeTypeManager().GetEntityTypeIDs(node_types.value()));
  }
  std::optional<SetOfEntityTypeIDs> edge_type_ids;
  if (edge_types) {
    edge_type_ids = KATANA_CHECKED(
        pg.GetEdgeTypeManager().GetEntityTypeIDs(edge_types.value()));
  }
  return MakeProjectedGraph(pg, node_type_ids, edge_type_ids);
}

/// Make a projected graph from a property graph. Shares state with
/// the original graph.
katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::MakeProjectedGraph(
    PropertyGraph& pg, std::optional<SetOfEntityTypeIDs> node_types,
    std::optional<SetOfEntityTypeIDs> edge_types) {
  const auto& topology = pg.topology();
  if (topology.empty()) {
    return MakeEmptyProjectedGraph(pg, katana::DynamicBitset{});
  }

  // calculate number of new nodes
  uint32_t num_new_nodes = 0;
  uint32_t num_new_edges = 0;

  katana::DynamicBitset bitset_nodes;
  bitset_nodes.resize(topology.NumNodes());

  NUMAArray<Node> original_to_projected_nodes_mapping;
  original_to_projected_nodes_mapping.allocateInterleaved(topology.NumNodes());

  if (!node_types) {
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

    katana::GAccumulator<uint32_t> accum_num_new_nodes;

    katana::do_all(katana::iterate(topology.Nodes()), [&](auto src) {
      for (auto type : node_types.value()) {
        if (pg.DoesNodeHaveType(src, type)) {
          accum_num_new_nodes += 1;
          bitset_nodes.set(src);
          // this sets the corresponding entry in the array to 1
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
      return MakeEmptyProjectedGraph(pg, bitset_nodes);
    }
  }

  // fill old to new nodes mapping
  katana::ParallelSTL::partial_sum(
      original_to_projected_nodes_mapping.begin(),
      original_to_projected_nodes_mapping.end(),
      original_to_projected_nodes_mapping.begin());

  NUMAArray<GraphTopology::PropertyIndex> projected_to_original_nodes_mapping;
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

  if (!edge_types) {
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
    katana::GAccumulator<uint32_t> accum_num_new_edges;

    katana::do_all(
        katana::iterate(Node{0}, Node{num_new_nodes}),
        [&](auto src) {
          auto old_src = projected_to_original_nodes_mapping[src];

          for (Edge e : topology.OutEdges(old_src)) {
            auto dest = topology.OutEdgeDst(e);
            if (bitset_nodes.test(dest)) {
              for (auto type : edge_types.value()) {
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
      return MakeEmptyEdgeProjectedGraph(
          pg, num_new_nodes, bitset_nodes,
          std::move(original_to_projected_nodes_mapping),
          std::move(projected_to_original_nodes_mapping));
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
  NUMAArray<GraphTopology::PropertyIndex> projected_to_original_edges_mapping;
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

  GraphTopology topo{
      std::move(out_indices), std::move(out_dests),
      std::move(projected_to_original_edges_mapping),
      std::move(projected_to_original_nodes_mapping)};

  // Using `new` to access a non-public constructor.
  return std::unique_ptr<PropertyGraph>(new PropertyGraph(
      pg, std::move(topo), std::move(original_to_projected_nodes_mapping),
      std::move(original_to_projected_edges_mapping), std::move(node_bitmask),
      std::move(edge_bitmask)));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Copy(
    const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties,
    katana::TxnContext* txn_ctx) const {
  // TODO(gill): This should copy the RDG in memory without reloading from storage.
  katana::RDGLoadOptions opts;
  opts.partition_id_to_load = partition_id();
  opts.node_properties = node_properties;
  opts.edge_properties = edge_properties;

  return Make(rdg_dir(), txn_ctx, opts);
}

katana::Result<void>
katana::PropertyGraph::Validate() {
  // TODO (thunt) check that arrow table sizes match topology
  // if (topology_.out_dests &&
  //    topology_.out_dests->length() != table->num_rows()) {
  //  return ErrorCode::InvalidArgument;
  //}
  // if (topology_.out_indices &&
  //    topology_.out_indices->length() != table->num_rows()) {
  //  return ErrorCode::InvalidArgument;
  //}

  uint64_t num_node_rows =
      static_cast<uint64_t>(rdg_->node_properties()->num_rows());
  if (num_node_rows == 0) {
    if ((rdg_->node_properties()->num_columns() != 0) && (NumNodes() != 0)) {
      return KATANA_ERROR(
          ErrorCode::AssertionFailed,
          "number of rows in node properties is 0 but "
          "the number of node properties is {} and the number of nodes is {}",
          rdg_->node_properties()->num_columns(), NumNodes());
    }
  } else if (num_node_rows != NumNodes()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "number of rows in node properties {} differs "
        "from the number of nodes {}",
        rdg_->node_properties()->num_rows(), NumNodes());
  }

  if (NumNodes() != node_entity_type_ids_->size()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "Number of nodes {} differs"
        "from the number of node IDs {} in the node type set ID array",
        NumNodes(), node_entity_type_ids_->size());
  }

  if (NumEdges() != edge_entity_type_ids_->size()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "Number of edges {} differs"
        "from the number of edge IDs {} in the edge type set ID array",
        NumEdges(), edge_entity_type_ids_->size());
  }

  uint64_t num_edge_rows =
      static_cast<uint64_t>(rdg_->edge_properties()->num_rows());
  if (num_edge_rows == 0) {
    if ((rdg_->edge_properties()->num_columns() != 0) && (NumEdges() != 0)) {
      return KATANA_ERROR(
          ErrorCode::AssertionFailed,
          "number of rows in edge properties is 0 but "
          "the number of edge properties is {} and the number of edges is {}",
          rdg_->edge_properties()->num_columns(), NumEdges());
    }
  } else if (num_edge_rows != NumEdges()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "number of rows in edge properties {} differs "
        "from the number of edges {}",
        rdg_->edge_properties()->num_rows(), NumEdges());
  }

  return katana::ResultSuccess();
}

/// Converts all uint8/bool properties into EntityTypeIDs
/// Only call this if every uint8/bool property should be considered a type
katana::Result<void>
katana::PropertyGraph::ConstructEntityTypeIDs(katana::TxnContext* txn_ctx) {
  // only relevant to actually construct when EntityTypeIDs are expected in properties
  // when EntityTypeIDs are not expected in properties then we have nothing to do here
  KATANA_LOG_WARN("Loading types from properties.");
  int64_t total_num_node_props = full_node_schema()->num_fields();
  for (int64_t i = 0; i < total_num_node_props; ++i) {
    if (full_node_schema()->field(i)->type()->Equals(arrow::uint8())) {
      KATANA_CHECKED_CONTEXT(
          EnsureNodePropertyLoaded(full_node_schema()->field(i)->name()),
          "loading uint8 property {} for type inference",
          full_node_schema()->field(i)->name());
    }
  }
  node_entity_type_manager_ = std::make_shared<EntityTypeManager>();
  node_entity_type_ids_ = std::make_shared<EntityTypeIDArray>();
  node_entity_type_ids_->allocateInterleaved(NumNodes());
  node_entity_data_ = node_entity_type_ids_->data();
  auto node_props_to_remove =
      KATANA_CHECKED(EntityTypeManager::AssignEntityTypeIDsFromProperties(
          NumNodes(), rdg_->node_properties(), node_entity_type_manager_.get(),
          node_entity_type_ids_.get()));
  for (const auto& node_prop : node_props_to_remove) {
    KATANA_CHECKED(RemoveNodeProperty(node_prop, txn_ctx));
  }

  int64_t total_num_edge_props = full_edge_schema()->num_fields();
  for (int64_t i = 0; i < total_num_edge_props; ++i) {
    if (full_edge_schema()->field(i)->type()->Equals(arrow::uint8())) {
      KATANA_CHECKED_CONTEXT(
          EnsureEdgePropertyLoaded(full_edge_schema()->field(i)->name()),
          "loading uint8 property {} for type inference",
          full_edge_schema()->field(i)->name());
    }
  }
  edge_entity_type_manager_ = std::make_shared<EntityTypeManager>();
  edge_entity_type_ids_ = std::make_shared<EntityTypeIDArray>();
  edge_entity_type_ids_->allocateInterleaved(NumEdges());
  edge_entity_data_ = edge_entity_type_ids_->data();
  auto edge_props_to_remove =
      KATANA_CHECKED(EntityTypeManager::AssignEntityTypeIDsFromProperties(
          NumEdges(), rdg_->edge_properties(), edge_entity_type_manager_.get(),
          edge_entity_type_ids_.get()));
  for (const auto& edge_prop : edge_props_to_remove) {
    KATANA_CHECKED(RemoveEdgeProperty(edge_prop, txn_ctx));
  }

  return katana::ResultSuccess();
}

katana::Result<katana::RDGTopology*>
katana::PropertyGraph::LoadTopology(const katana::RDGTopology& shadow) {
  if (IsTransformed()) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument,
        "Transformation topologies are not persisted yet.");
  }

  katana::RDGTopology* topo = KATANA_CHECKED(rdg_->GetTopology(shadow));
  if (NumEdges() != topo->num_edges() || NumNodes() != topo->num_nodes()) {
    KATANA_LOG_WARN(
        "RDG found topology matching description, but num_edge/num_node does "
        "not match csr topology");
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "no matching topology found");
  }
  return topo;
}

katana::Result<void>
katana::PropertyGraph::DoWriteTopologies() {
  // Since PGViewCache doesn't manage the main csr topology, see if we need to store it now
  katana::RDGTopology shadow = KATANA_CHECKED(katana::RDGTopology::Make(
      topology().AdjData(), topology().NumNodes(), topology().DestData(),
      topology().NumEdges(), katana::RDGTopology::TopologyKind::kCSR,
      katana::RDGTopology::TransposeKind::kNo,
      katana::RDGTopology::EdgeSortKind::kAny,
      katana::RDGTopology::NodeSortKind::kAny));

  rdg_->UpsertTopology(std::move(shadow));

  std::vector<katana::RDGTopology> topologies =
      KATANA_CHECKED(pg_view_cache_.ToRDGTopology());
  for (size_t i = 0; i < topologies.size(); i++) {
    rdg_->UpsertTopology(std::move(topologies.at(i)));
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::DoWrite(
    katana::RDGHandle handle, const std::string& command_line,
    katana::RDG::RDGVersioningPolicy versioning_action,
    katana::TxnContext* txn_ctx) {
  KATANA_LOG_DEBUG(
      " node array valid: {}, edge array valid: {}",
      rdg_->node_entity_type_id_array_file_storage().Valid(),
      rdg_->edge_entity_type_id_array_file_storage().Valid());

  KATANA_CHECKED(DoWriteTopologies());

  //TODO(emcginnis): we don't actually have any lifetime tracking for the in memory
  // entity_type_id arrays, which means we don't actually know when the array
  // on disk is out of date and we should write. For now, just always write the file.
  // This is correct, but wasteful.
  std::unique_ptr<katana::FileFrame> node_entity_type_id_array_res =
      KATANA_CHECKED(WriteEntityTypeIDsArray(*node_entity_type_ids_));

  std::unique_ptr<katana::FileFrame> edge_entity_type_id_array_res =
      KATANA_CHECKED(WriteEntityTypeIDsArray(*edge_entity_type_ids_));

  return rdg_->Store(
      handle, command_line, versioning_action,
      std::move(node_entity_type_id_array_res),
      std::move(edge_entity_type_id_array_res), GetNodeTypeManager(),
      GetEdgeTypeManager(), txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::ConductWriteOp(
    const katana::URI& uri, const std::string& command_line,
    katana::RDG::RDGVersioningPolicy versioning_action,
    katana::TxnContext* txn_ctx) {
  katana::RDGManifest manifest =
      KATANA_CHECKED(katana::FindManifest(uri, txn_ctx));

  katana::RDGHandle rdg_handle =
      KATANA_CHECKED(katana::Open(std::move(manifest), katana::kReadWrite));
  auto new_file = std::make_unique<katana::RDGFile>(rdg_handle);

  KATANA_CHECKED(DoWrite(*new_file, command_line, versioning_action, txn_ctx));

  file_ = std::move(new_file);

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::WriteView(
    const katana::URI& uri, const std::string& command_line,
    katana::TxnContext* txn_ctx) {
  return ConductWriteOp(
      uri, command_line, katana::RDG::RDGVersioningPolicy::RetainVersion,
      txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::WriteGraph(
    const katana::URI& uri, const std::string& command_line,
    katana::TxnContext* txn_ctx) {
  return ConductWriteOp(
      uri, command_line, katana::RDG::RDGVersioningPolicy::IncrementVersion,
      txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::Commit(
    const std::string& command_line, katana::TxnContext* txn_ctx) {
  if (IsTransformed()) {
    return parent_->Commit(command_line, txn_ctx);
  }

  if (file_ == nullptr) {
    if (rdg_->rdg_dir().empty()) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument, "RDG commit but rdg_dir_ is empty");
    }
    return WriteGraph(rdg_->rdg_dir(), command_line, txn_ctx);
  }
  return DoWrite(
      *file_, command_line, katana::RDG::RDGVersioningPolicy::IncrementVersion,
      txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::WriteView(
    const std::string& command_line, katana::TxnContext* txn_ctx) {
  if (IsTransformed()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "PropertyGraph::WriteView should not be called on a projected graph");
  }
  // WriteView occurs once, and only before any Commit/Write operation
  KATANA_LOG_DEBUG_ASSERT(file_ == nullptr);
  return WriteView(rdg_->rdg_dir(), command_line, txn_ctx);
}

bool
katana::PropertyGraph::Equals(const PropertyGraph* other) const {
  if (!topology().Equals(other->topology())) {
    return false;
  }

  if (!GetNodeTypeManager().IsIsomorphicTo(other->GetNodeTypeManager())) {
    return false;
  }

  if (!GetEdgeTypeManager().IsIsomorphicTo(other->GetEdgeTypeManager())) {
    return false;
  }

  // The TypeIDs can change, but their string interpretation cannot
  if (node_entity_type_ids_->size() != other->node_entity_type_ids_->size()) {
    return false;
  }
  for (size_t i = 0; i < node_entity_type_ids_->size(); ++i) {
    auto tns =
        GetNodeTypeManager().EntityTypeToTypeNameSet(node_entity_data_[i]);
    auto otns = other->GetNodeTypeManager().EntityTypeToTypeNameSet(
        other->node_entity_data_[i]);
    if (tns != otns) {
      return false;
    }
  }

  // The TypeIDs can change, but their string interpretation cannot
  if (edge_entity_type_ids_->size() != other->edge_entity_type_ids_->size()) {
    return false;
  }
  for (size_t i = 0; i < edge_entity_type_ids_->size(); ++i) {
    auto tns =
        GetEdgeTypeManager().EntityTypeToTypeNameSet(edge_entity_data_[i]);
    auto otns = other->GetEdgeTypeManager().EntityTypeToTypeNameSet(
        other->edge_entity_data_[i]);
    if (tns != otns) {
      return false;
    }
  }

  const auto& node_props = rdg_->node_properties();
  const auto& edge_props = rdg_->edge_properties();
  const auto& other_node_props = other->rdg_->node_properties();
  const auto& other_edge_props = other->rdg_->edge_properties();
  if (node_props->num_columns() != other_node_props->num_columns()) {
    return false;
  }
  if (edge_props->num_columns() != other_edge_props->num_columns()) {
    return false;
  }
  for (const auto& prop_name : node_props->ColumnNames()) {
    if (!node_props->GetColumnByName(prop_name)->Equals(
            other_node_props->GetColumnByName(prop_name))) {
      return false;
    }
  }
  for (const auto& prop_name : edge_props->ColumnNames()) {
    if (!edge_props->GetColumnByName(prop_name)->Equals(
            other_edge_props->GetColumnByName(prop_name))) {
      return false;
    }
  }
  return true;
}

std::string
katana::PropertyGraph::ReportDiff(const PropertyGraph* other) const {
  fmt::memory_buffer buf;
  if (!topology().Equals(other->topology())) {
    fmt::format_to(
        std::back_inserter(buf),
        "Topologies differ nodes/edges {}/{} vs. {}/{}\n",
        topology().NumNodes(), topology().NumEdges(),
        other->topology().NumNodes(), other->topology().NumEdges());
  } else {
    fmt::format_to(std::back_inserter(buf), "Topologies match!\n");
  }

  fmt::format_to(std::back_inserter(buf), "NodeEntityTypeManager Diff:\n");
  fmt::format_to(
      std::back_inserter(buf),
      GetNodeTypeManager().ReportDiff(other->GetNodeTypeManager()));
  fmt::format_to(std::back_inserter(buf), "EdgeEntityTypeManager Diff:\n");
  fmt::format_to(
      std::back_inserter(buf),
      GetEdgeTypeManager().ReportDiff(other->GetEdgeTypeManager()));

  // The TypeIDs can change, but their string interpretation cannot
  bool match = true;
  if (node_entity_type_ids_->size() != other->node_entity_type_ids_->size()) {
    fmt::format_to(
        std::back_inserter(buf),
        "node_entity_type_ids differ. size {} vs. {}\n",
        node_entity_type_ids_->size(), other->node_entity_type_ids_->size());
    match = false;
  } else {
    for (size_t i = 0; i < node_entity_type_ids_->size(); ++i) {
      auto tns_res =
          GetNodeTypeManager().EntityTypeToTypeNameSet(node_entity_data_[i]);
      auto otns_res = other->GetNodeTypeManager().EntityTypeToTypeNameSet(
          other->node_entity_data_[i]);
      if (!tns_res || !otns_res) {
        fmt::format_to(
            std::back_inserter(buf),
            "node error types index {} entity lhs {} entity rhs_{}\n", i,
            node_entity_data_[i], other->node_entity_data_[i]);
        match = false;
        break;
      }
      auto tns = tns_res.value();
      auto otns = otns_res.value();
      if (tns != otns) {
        fmt::format_to(
            std::back_inserter(buf),
            "node_entity_type_ids differ. {:4} {} {} {} {}\n", i,
            node_entity_data_[i], fmt::join(tns, ", "),
            other->node_entity_data_[i], fmt::join(otns, ", "));
        match = false;
      }
    }
  }
  if (match) {
    fmt::format_to(std::back_inserter(buf), "node_entity_type_ids Match!\n");
  }

  // The TypeIDs can change, but their string interpretation cannot
  match = true;
  if (edge_entity_type_ids_->size() != other->edge_entity_type_ids_->size()) {
    fmt::format_to(
        std::back_inserter(buf),
        "edge_entity_type_ids differ. size {} vs. {}\n",
        edge_entity_type_ids_->size(), other->edge_entity_type_ids_->size());
    match = false;
  } else {
    for (size_t i = 0; i < edge_entity_type_ids_->size(); ++i) {
      auto tns_res =
          GetEdgeTypeManager().EntityTypeToTypeNameSet(edge_entity_data_[i]);
      auto otns_res = other->GetEdgeTypeManager().EntityTypeToTypeNameSet(
          other->edge_entity_data_[i]);
      if (!tns_res || !otns_res) {
        fmt::format_to(
            std::back_inserter(buf),
            "edge error types index {} entity lhs {} entity rhs_{}\n", i,
            edge_entity_data_[i], other->edge_entity_data_[i]);
        match = false;
        break;
      }
      auto tns = tns_res.value();
      auto otns = otns_res.value();
      if (tns != otns) {
        fmt::format_to(
            std::back_inserter(buf),
            "edge_entity_type_ids differ. {:4} {} {} {} {}\n", i,
            edge_entity_data_[i], fmt::join(tns, ", "),
            other->edge_entity_data_[i], fmt::join(otns, ", "));
        match = false;
      }
    }
  }
  if (match) {
    fmt::format_to(std::back_inserter(buf), "edge_entity_type_ids Match!\n");
  }

  const auto& node_props = rdg_->node_properties();
  const auto& edge_props = rdg_->edge_properties();
  const auto& other_node_props = other->rdg_->node_properties();
  const auto& other_edge_props = other->rdg_->edge_properties();
  if (node_props->num_columns() != other_node_props->num_columns()) {
    fmt::format_to(
        std::back_inserter(buf), "Number of node properties differ {} vs. {}\n",
        node_props->num_columns(), other_node_props->num_columns());
  }
  if (edge_props->num_columns() != other_edge_props->num_columns()) {
    fmt::format_to(
        std::back_inserter(buf), "Number of edge properties differ {} vs. {}\n",
        edge_props->num_columns(), other_edge_props->num_columns());
  }
  for (const auto& prop_name : node_props->ColumnNames()) {
    auto other_col = other_node_props->GetColumnByName(prop_name);
    auto my_col = node_props->GetColumnByName(prop_name);
    if (other_col == nullptr) {
      fmt::format_to(
          std::back_inserter(buf), "Only first has node property {}\n",
          prop_name);
    } else if (!my_col->Equals(other_col)) {
      fmt::format_to(
          std::back_inserter(buf), "Node property {:15} {:12} differs\n",
          prop_name, fmt::format("({})", my_col->type()->name()));
      if (my_col->length() != other_col->length()) {
        fmt::format_to(
            std::back_inserter(buf), " size {}/{}\n", my_col->length(),
            other_col->length());
      } else {
        DiffFormatTo(buf, my_col, other_col);
      }
    } else {
      fmt::format_to(
          std::back_inserter(buf), "Node property {:15} {:12} matches!\n",
          prop_name, fmt::format("({})", my_col->type()->name()));
    }
  }
  for (const auto& prop_name : edge_props->ColumnNames()) {
    auto other_col = other_edge_props->GetColumnByName(prop_name);
    auto my_col = edge_props->GetColumnByName(prop_name);
    if (other_col == nullptr) {
      fmt::format_to(
          std::back_inserter(buf), "Only first has edge property {}\n",
          prop_name);
    } else if (!edge_props->GetColumnByName(prop_name)->Equals(
                   other_edge_props->GetColumnByName(prop_name))) {
      fmt::format_to(
          std::back_inserter(buf), "Edge property {:15} {:12} differs\n",
          prop_name, fmt::format("({})", my_col->type()->name()));
      if (my_col->length() != other_col->length()) {
        fmt::format_to(
            std::back_inserter(buf), " size {}/{}\n", my_col->length(),
            other_col->length());
      } else {
        DiffFormatTo(buf, my_col, other_col);
      }
    } else {
      fmt::format_to(
          std::back_inserter(buf), "Edge property {:15} {:12} matches!\n",
          prop_name, fmt::format("({})", my_col->type()->name()));
    }
  }
  return std::string(buf.begin(), buf.end());
}

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
katana::PropertyGraph::GetNodeProperty(const std::string& name) const {
  auto ret = rdg_->node_properties()->GetColumnByName(name);
  if (ret) {
    return MakeResult(std::move(ret));
  }
  return KATANA_ERROR(
      ErrorCode::PropertyNotFound, "node property does not exist: {}", name);
}

katana::Result<katana::URI>
katana::PropertyGraph::GetNodePropertyStorageLocation(
    const std::string& name) const {
  return rdg_->GetNodePropertyStorageLocation(name);
}

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
katana::PropertyGraph::GetEdgeProperty(const std::string& name) const {
  auto ret = rdg_->edge_properties()->GetColumnByName(name);
  if (ret) {
    return MakeResult(std::move(ret));
  }
  return KATANA_ERROR(
      ErrorCode::PropertyNotFound, "edge property does not exist: {}", name);
}

katana::Result<katana::URI>
katana::PropertyGraph::GetEdgePropertyStorageLocation(
    const std::string& name) const {
  return rdg_->GetEdgePropertyStorageLocation(name);
}

katana::Result<void>
katana::PropertyGraph::Write(
    const katana::URI& rdg_dir, const std::string& command_line,
    katana::TxnContext* txn_ctx) {
  if (IsTransformed()) {
    return parent_->Write(rdg_dir, command_line, txn_ctx);
  }

  if (auto res = katana::Create(rdg_dir); !res) {
    return res.error();
  }
  return WriteGraph(rdg_dir, command_line, txn_ctx);
}

// We do this to avoid a virtual call, since this method is often on a hot path.
katana::GraphTopology::PropertyIndex
katana::PropertyGraph::GetEdgePropertyIndexFromOutEdge(
    const Edge& eid) const noexcept {
  return topology().GetEdgePropertyIndexFromOutEdge(eid);
}

// We do this to avoid a virtual call, since this method is often on a hot path.
katana::GraphTopology::PropertyIndex
katana::PropertyGraph::GetNodePropertyIndex(const Node& nid) const noexcept {
  return topology().GetNodePropertyIndex(nid);
}

katana::Result<void>
katana::PropertyGraph::AddNodeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("adding empty node prop table");
    return ResultSuccess();
  }
  if (NumOriginalNodes() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        NumOriginalNodes(), props->num_rows());
  }
  return rdg_->AddNodeProperties(props, txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::UpsertNodeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("upsert empty node prop table");
    return ResultSuccess();
  }
  if (NumOriginalNodes() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        NumOriginalNodes(), props->num_rows());
  }
  return rdg_->UpsertNodeProperties(props, txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::RemoveNodeProperty(int i, katana::TxnContext* txn_ctx) {
  return rdg_->RemoveNodeProperty(i, txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::RemoveNodeProperty(
    const std::string& prop_name, katana::TxnContext* txn_ctx) {
  auto col_names = rdg_->node_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
  if (pos != col_names.cend()) {
    return rdg_->RemoveNodeProperty(
        std::distance(col_names.cbegin(), pos), txn_ctx);
  }
  return katana::ErrorCode::PropertyNotFound;
}

katana::Result<void>
katana::PropertyGraph::LoadNodeProperty(const std::string& name, int i) {
  return rdg_->LoadNodeProperty(name, i);
}
/// Load a node property by name if it is absent and append its column to
/// the table do nothing otherwise
katana::Result<void>
katana::PropertyGraph::EnsureNodePropertyLoaded(const std::string& name) {
  if (HasNodeProperty(name)) {
    return katana::ResultSuccess();
  }
  return LoadNodeProperty(name);
}

katana::Result<void>
katana::PropertyGraph::UnloadNodeProperty(const std::string& prop_name) {
  return rdg_->UnloadNodeProperty(prop_name);
}

katana::Result<void>
katana::PropertyGraph::AddEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("adding empty edge prop table");
    return ResultSuccess();
  }
  if (NumOriginalEdges() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        NumOriginalEdges(), props->num_rows());
  }
  return rdg_->AddEdgeProperties(props, txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::UpsertEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("upsert empty edge prop table");
    return ResultSuccess();
  }
  if (NumOriginalEdges() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        NumOriginalEdges(), props->num_rows());
  }
  return rdg_->UpsertEdgeProperties(props, txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::RemoveEdgeProperty(int i, katana::TxnContext* txn_ctx) {
  return rdg_->RemoveEdgeProperty(i, txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::RemoveEdgeProperty(
    const std::string& prop_name, katana::TxnContext* txn_ctx) {
  auto col_names = rdg_->edge_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
  if (pos != col_names.cend()) {
    return rdg_->RemoveEdgeProperty(
        std::distance(col_names.cbegin(), pos), txn_ctx);
  }
  return katana::ErrorCode::PropertyNotFound;
}

katana::Result<void>
katana::PropertyGraph::UnloadEdgeProperty(const std::string& prop_name) {
  return rdg_->UnloadEdgeProperty(prop_name);
}

katana::Result<void>
katana::PropertyGraph::LoadEdgeProperty(const std::string& name, int i) {
  return rdg_->LoadEdgeProperty(name, i);
}

/// Load an edge property by name if it is absent and append its column to
/// the table do nothing otherwise
katana::Result<void>
katana::PropertyGraph::EnsureEdgePropertyLoaded(const std::string& name) {
  if (HasEdgeProperty(name)) {
    return katana::ResultSuccess();
  }
  return LoadEdgeProperty(name);
}

// Build an index over nodes.
katana::Result<void>
katana::PropertyGraph::MakeNodeIndex(const std::string& property_name) {
  for (const auto& existing_index : node_indexes_) {
    if (existing_index->property_name() == property_name) {
      return KATANA_ERROR(
          katana::ErrorCode::AlreadyExists,
          "Index already exists for column {}", property_name);
    }
  }

  // Get a view of the property.
  std::shared_ptr<arrow::ChunkedArray> chunked_property =
      KATANA_CHECKED(GetNodeProperty(property_name));
  KATANA_LOG_ASSERT(chunked_property->num_chunks() == 1);
  std::shared_ptr<arrow::Array> property = chunked_property->chunk(0);

  // Create an index based on the type of the field.
  std::shared_ptr<katana::EntityIndex<GraphTopology::Node>> index =
      KATANA_CHECKED(katana::MakeTypedEntityIndex<katana::GraphTopology::Node>(
          property_name, NumNodes(), property));

  KATANA_CHECKED(index->BuildFromProperty());

  node_indexes_.push_back(std::move(index));

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::DeleteNodeIndex(const std::string& property_name) {
  for (auto it = node_indexes_.begin(); it != node_indexes_.end(); it++) {
    if ((*it)->property_name() == property_name) {
      node_indexes_.erase(it);
      return katana::ResultSuccess();
    }
  }

  // TODO(Chak-Pong) make deleteNodeIndex always successful
  //  before index existence check is available from python side
  //  return KATANA_ERROR(katana::ErrorCode::NotFound, "node index not found");
  KATANA_LOG_WARN("the following node index not found: {}", property_name);
  return katana::ResultSuccess();
}

// Build an index over edges.
katana::Result<void>
katana::PropertyGraph::MakeEdgeIndex(const std::string& property_name) {
  for (const auto& existing_index : edge_indexes_) {
    if (existing_index->property_name() == property_name) {
      return KATANA_ERROR(
          katana::ErrorCode::AlreadyExists,
          "Index already exists for column {}", property_name);
    }
  }

  // Get a view of the property.
  std::shared_ptr<arrow::ChunkedArray> chunked_property =
      KATANA_CHECKED(GetEdgeProperty(property_name));
  KATANA_LOG_ASSERT(chunked_property->num_chunks() == 1);
  std::shared_ptr<arrow::Array> property = chunked_property->chunk(0);

  // Create an index based on the type of the field.
  std::unique_ptr<katana::EntityIndex<katana::GraphTopology::Edge>> index =
      KATANA_CHECKED(katana::MakeTypedEntityIndex<katana::GraphTopology::Edge>(
          property_name, NumEdges(), property));

  KATANA_CHECKED(index->BuildFromProperty());

  edge_indexes_.push_back(std::move(index));

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::DeleteEdgeIndex(const std::string& property_name) {
  for (auto it = edge_indexes_.begin(); it != edge_indexes_.end(); it++) {
    if ((*it)->property_name() == property_name) {
      edge_indexes_.erase(it);
      return katana::ResultSuccess();
    }
  }
  return KATANA_ERROR(katana::ErrorCode::NotFound, "edge index not found");
}

katana::Result<std::unique_ptr<katana::NUMAArray<uint64_t>>>
katana::SortAllEdgesByDest(katana::PropertyGraph* pg) {
  // TODO(amber): This function will soon change so that it produces a new sorted
  // topology instead of modifying an existing one. The const_cast will go away
  const auto& topo = pg->topology();

  auto permutation_vec = std::make_unique<katana::NUMAArray<uint64_t>>();
  permutation_vec->allocateInterleaved(topo.NumEdges());
  katana::ParallelSTL::iota(
      permutation_vec->begin(), permutation_vec->end(), uint64_t{0});

  auto* out_dests_data = const_cast<GraphTopology::Node*>(topo.DestData());

  katana::do_all(
      katana::iterate(pg->topology().Nodes()),
      [&](GraphTopology::Node n) {
        const auto e_beg = *pg->topology().OutEdges(n).begin();
        const auto e_end = *pg->topology().OutEdges(n).end();

        auto sort_iter_beg = katana::make_zip_iterator(
            out_dests_data + e_beg, permutation_vec->begin() + e_beg);
        auto sort_iter_end = katana::make_zip_iterator(
            out_dests_data + e_end, permutation_vec->begin() + e_end);

        std::sort(
            sort_iter_beg, sort_iter_end,
            [&](const auto& tup1, const auto& tup2) {
              auto d1 = std::get<0>(tup1);
              auto d2 = std::get<0>(tup2);
              static_assert(std::is_same_v<decltype(d1), GraphTopology::Node>);
              static_assert(std::is_same_v<decltype(d1), GraphTopology::Node>);
              return d1 < d2;
            });
      },
      katana::steal());

  return std::unique_ptr<katana::NUMAArray<uint64_t>>(
      std::move(permutation_vec));
}

// TODO(amber): make this a method of a sorted topology class in the near future
// TODO(amber): this method should return an edge_iterator
katana::GraphTopology::Edge
katana::FindEdgeSortedByDest(
    const PropertyGraph* graph, const GraphTopology::Node src,
    const GraphTopology::Node dst) {
  const auto& topo = graph->topology();
  auto e_range = topo.OutEdges(src);

  constexpr size_t kBinarySearchThreshold = 64;

  if (e_range.size() <= kBinarySearchThreshold) {
    auto iter = std::find_if(
        e_range.begin(), e_range.end(), [&](const GraphTopology::Edge& e) {
          return topo.OutEdgeDst(e) == dst;
        });

    return *iter;

  } else {
    auto iter = std::lower_bound(
        e_range.begin(), e_range.end(), dst,
        internal::EdgeDestComparator<GraphTopology>{&topo});

    return topo.OutEdgeDst(*iter) == dst ? *iter : *e_range.end();
  }
}

// TODO(amber): this method should return a new sorted topology
katana::Result<void>
katana::SortNodesByDegree(katana::PropertyGraph* pg) {
  const auto& topo = pg->topology();

  uint64_t num_nodes = topo.NumNodes();
  uint64_t num_edges = topo.NumEdges();

  using DegreeNodePair = std::pair<uint64_t, uint32_t>;
  katana::NUMAArray<DegreeNodePair> dn_pairs;
  dn_pairs.allocateInterleaved(num_nodes);

  katana::do_all(katana::iterate(topo.Nodes()), [&](auto node) {
    size_t node_degree = topo.OutDegree(node);
    dn_pairs[node] = DegreeNodePair(node_degree, node);
  });

  // sort by degree (first item)
  katana::ParallelSTL::sort(
      dn_pairs.begin(), dn_pairs.end(), std::greater<DegreeNodePair>());

  // create mapping, get degrees out to another vector to get prefix sum
  katana::NUMAArray<uint32_t> old_to_new_mapping;
  old_to_new_mapping.allocateInterleaved(num_nodes);

  katana::NUMAArray<uint64_t> new_prefix_sum;
  new_prefix_sum.allocateInterleaved(num_nodes);

  katana::do_all(katana::iterate(uint64_t{0}, num_nodes), [&](uint64_t index) {
    // save degree, which is pair.first
    new_prefix_sum[index] = dn_pairs[index].first;
    // save mapping; original index is in .second, map it to current index
    old_to_new_mapping[dn_pairs[index].second] = index;
  });

  katana::ParallelSTL::partial_sum(
      new_prefix_sum.begin(), new_prefix_sum.end(), new_prefix_sum.begin());

  katana::NUMAArray<uint32_t> new_out_dest;
  new_out_dest.allocateInterleaved(num_edges);

  auto* out_dests_data = const_cast<GraphTopology::Node*>(topo.DestData());
  auto* out_indices_data = const_cast<GraphTopology::Edge*>(topo.AdjData());

  katana::do_all(
      katana::iterate(topo.Nodes()),
      [&](auto old_node_id) {
        uint32_t new_node_id = old_to_new_mapping[old_node_id];

        // get the start location of this reindex'd nodes edges
        uint64_t new_out_index =
            (new_node_id == 0) ? 0 : new_prefix_sum[new_node_id - 1];

        // construct the graph, reindexing as it goes along
        for (auto e : topo.OutEdges(old_node_id)) {
          // get destination, reindex
          uint32_t old_edge_dest = out_dests_data[e];
          uint32_t new_edge_dest = old_to_new_mapping[old_edge_dest];

          new_out_dest[new_out_index] = new_edge_dest;

          new_out_index++;
        }
        // this assert makes sure reindex was correct + makes sure all edges
        // are accounted for
        KATANA_LOG_DEBUG_ASSERT(new_out_index == new_prefix_sum[new_node_id]);
      },
      katana::steal());

  //Update the underlying PropertyGraph topology
  // TODO(amber): eliminate these copies since we will be returning a new topology
  katana::do_all(katana::iterate(uint64_t{0}, num_nodes), [&](auto node_id) {
    out_indices_data[node_id] = new_prefix_sum[node_id];
  });

  katana::do_all(katana::iterate(uint64_t{0}, num_edges), [&](auto edge_id) {
    out_dests_data[edge_id] = new_out_dest[edge_id];
  });

  return katana::ResultSuccess();
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::CreateSymmetricGraph(katana::PropertyGraph* pg) {
  const GraphTopology& topology = pg->topology();
  if (topology.NumNodes() == 0) {
    return std::make_unique<PropertyGraph>();
  }

  // New symmetric graph topology
  katana::NUMAArray<uint64_t> out_indices;
  katana::NUMAArray<uint32_t> out_dests;

  out_indices.allocateInterleaved(topology.NumNodes());
  // Store the out-degree of nodes from original graph
  katana::do_all(katana::iterate(topology.Nodes()), [&](auto n) {
    out_indices[n] = topology.OutDegree(n);
  });

  katana::do_all(
      katana::iterate(topology.Nodes()),
      [&](auto n) {
        // update the out_indices for the symmetric topology
        for (auto e : topology.OutEdges(n)) {
          auto dest = topology.OutEdgeDst(e);
          // Do not add reverse edge for self-loops
          if (n != dest) {
            __sync_fetch_and_add(&(out_indices[dest]), 1);
          }
        }
      },
      katana::steal());

  // Compute prefix sum
  katana::ParallelSTL::partial_sum(
      out_indices.begin(), out_indices.end(), out_indices.begin());

  uint64_t num_nodes_symmetric = topology.NumNodes();
  uint64_t num_edges_symmetric = out_indices[num_nodes_symmetric - 1];

  katana::NUMAArray<uint64_t> out_dests_offset;
  out_dests_offset.allocateInterleaved(topology.NumNodes());
  // Temp NUMAArray for computing new destination positions
  out_dests_offset[0] = 0;
  katana::do_all(
      katana::iterate(uint64_t{1}, topology.NumNodes()),
      [&](uint64_t n) { out_dests_offset[n] = out_indices[n - 1]; },
      katana::no_stats());

  out_dests.allocateInterleaved(num_edges_symmetric);
  // Update graph topology with the original edges + reverse edges
  katana::do_all(
      katana::iterate(topology.Nodes()),
      [&](auto src) {
        // get all outgoing edges (excluding self edges) of a particular
        // node and add reverse edges.
        for (GraphTopology::Edge e : topology.OutEdges(src)) {
          // e = start index into edge array for a particular node
          // destination node
          auto dest = topology.OutEdgeDst(e);

          // Add original edge
          auto e_new_src = __sync_fetch_and_add(&(out_dests_offset[src]), 1);
          out_dests[e_new_src] = dest;

          // Do not add reverse edge for self-loops
          if (dest != src) {
            // Add reverse edge
            auto e_new_dst = __sync_fetch_and_add(&(out_dests_offset[dest]), 1);
            out_dests[e_new_dst] = src;
          }
        }
      },
      katana::no_stats());

  GraphTopology sym_topo(std::move(out_indices), std::move(out_dests));
  return katana::PropertyGraph::Make(std::move(sym_topo));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::CreateTransposeGraphTopology(const GraphTopology& topology) {
  if (topology.NumNodes() == 0) {
    return std::make_unique<PropertyGraph>();
  }

  katana::NUMAArray<GraphTopology::Edge> out_indices;
  katana::NUMAArray<GraphTopology::Node> out_dests;

  out_indices.allocateInterleaved(topology.NumNodes());
  out_dests.allocateInterleaved(topology.NumEdges());

  // Initialize the new topology indices
  katana::do_all(
      katana::iterate(uint64_t{0}, topology.NumNodes()),
      [&](uint64_t n) { out_indices[n] = uint64_t{0}; }, katana::no_stats());

  // Keep a copy of old destinaton ids and compute number of
  // in-coming edges for the new prefix sum of out_indices.
  katana::do_all(
      katana::iterate(topology.OutEdges()),
      [&](auto e) {
        // Counting outgoing edges in the tranpose graph by
        // counting incoming edges in the original graph
        auto dest = topology.OutEdgeDst(e);
        __sync_add_and_fetch(&(out_indices[dest]), 1);
      },
      katana::no_stats());

  // Prefix sum calculation of the edge index array
  katana::ParallelSTL::partial_sum(
      out_indices.begin(), out_indices.end(), out_indices.begin());

  katana::NUMAArray<uint64_t> out_dests_offset;
  out_dests_offset.allocateInterleaved(topology.NumNodes());

  // temporary buffer for storing the starting point of each node's transpose
  // adjacency
  out_dests_offset[0] = 0;
  katana::do_all(
      katana::iterate(uint64_t{1}, topology.NumNodes()),
      [&](uint64_t n) { out_dests_offset[n] = out_indices[n - 1]; },
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
        }
      },
      katana::no_stats());

  GraphTopology transpose_topo{std::move(out_indices), std::move(out_dests)};
  return katana::PropertyGraph::Make(std::move(transpose_topo));
}

bool
katana::PropertyGraph::HasNodeIndex(const std::string& property_name) const {
  for (const auto& index : node_indexes()) {
    if (index->property_name() == property_name) {
      return true;
    }
  }
  return false;
}

katana::Result<
    std::shared_ptr<katana::EntityIndex<katana::GraphTopology::Node>>>
katana::PropertyGraph::GetNodeIndex(const std::string& property_name) const {
  for (const auto& index : node_indexes()) {
    if (index->property_name() == property_name) {
      return index;
    }
  }
  return KATANA_ERROR(katana::ErrorCode::NotFound, "node index not found");
}

bool
katana::PropertyGraph::HasEdgeIndex(const std::string& property_name) const {
  for (const auto& index : edge_indexes()) {
    if (index->property_name() == property_name) {
      return true;
    }
  }
  return false;
}

katana::Result<
    std::shared_ptr<katana::EntityIndex<katana::GraphTopology::Edge>>>
katana::PropertyGraph::GetEdgeIndex(const std::string& property_name) const {
  for (const auto& index : edge_indexes()) {
    if (index->property_name() == property_name) {
      return index;
    }
  }
  return KATANA_ERROR(katana::ErrorCode::NotFound, "edge index not found");
}

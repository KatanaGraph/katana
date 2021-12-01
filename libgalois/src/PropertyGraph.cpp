#include "katana/PropertyGraph.h"

#include <stdio.h>
#include <sys/mman.h>

#include <memory>
#include <utility>
#include <vector>

#include <arrow/array.h>

#include "katana/ArrowInterchange.h"
#include "katana/GraphTopology.h"
#include "katana/Iterators.h"
#include "katana/Logging.h"
#include "katana/Loops.h"
#include "katana/NUMAArray.h"
#include "katana/PerThreadStorage.h"
#include "katana/Platform.h"
#include "katana/Properties.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FileFrame.h"
#include "tsuba/RDG.h"
#include "tsuba/RDGManifest.h"
#include "tsuba/RDGPrefix.h"
#include "tsuba/RDGTopology.h"
#include "tsuba/tsuba.h"

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
    const tsuba::FileView& file_view, bool is_uint16_t_entity_type_ids) {
  const auto* data = file_view.ptr<tsuba::EntityTypeIDArrayHeader>();
  const auto header = data[0];

  if (file_view.size() == 0) {
    return katana::ErrorCode::InvalidArgument;
  }

  // allocate type IDs array
  katana::PropertyGraph::EntityTypeIDArray entity_type_id_array;
  entity_type_id_array.allocateInterleaved(header.size);

  if (is_uint16_t_entity_type_ids) {
    const katana::EntityTypeID* type_IDs_array =
        reinterpret_cast<const katana::EntityTypeID*>(&data[1]);

    KATANA_LOG_DEBUG_ASSERT(type_IDs_array != nullptr);

    katana::ParallelSTL::copy(
        &type_IDs_array[0], &type_IDs_array[header.size],
        entity_type_id_array.begin());
  } else {
    // On disk format is still uint8_t EntityTypeIDs
    const uint8_t* type_IDs_array = reinterpret_cast<const uint8_t*>(&data[1]);

    KATANA_LOG_DEBUG_ASSERT(type_IDs_array != nullptr);

    katana::ParallelSTL::copy(
        &type_IDs_array[0], &type_IDs_array[header.size],
        entity_type_id_array.begin());
  }

  return katana::MakeResult(std::move(entity_type_id_array));
}

katana::Result<std::unique_ptr<tsuba::FileFrame>>
WriteEntityTypeIDsArray(
    const katana::NUMAArray<katana::EntityTypeID>& entity_type_id_array) {
  auto ff = std::make_unique<tsuba::FileFrame>();

  if (auto res = ff->Init(); !res) {
    return res.error();
  }

  tsuba::EntityTypeIDArrayHeader data[1] = {
      {.size = entity_type_id_array.size()}};
  arrow::Status aro_sts =
      ff->Write(&data, sizeof(tsuba::EntityTypeIDArrayHeader));

  if (!aro_sts.ok()) {
    return tsuba::ArrowToTsuba(aro_sts.code());
  }

  if (entity_type_id_array.size()) {
    const katana::EntityTypeID* raw = entity_type_id_array.data();
    auto buf = arrow::Buffer::Wrap(raw, entity_type_id_array.size());
    aro_sts = ff->Write(buf);
    if (!aro_sts.ok()) {
      return tsuba::ArrowToTsuba(aro_sts.code());
    }
  }
  return std::unique_ptr<tsuba::FileFrame>(std::move(ff));
}

katana::PropertyGraph::EntityTypeIDArray
MakeDefaultEntityTypeIDArray(size_t vec_sz) {
  katana::PropertyGraph::EntityTypeIDArray type_ids;
  type_ids.allocateInterleaved(vec_sz);
  katana::ParallelSTL::fill(
      type_ids.begin(), type_ids.end(), katana::kUnknownEntityType);
  return type_ids;
}

}  // namespace

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg) {
  // find & map the default csr topology
  tsuba::RDGTopology shadow_csr = tsuba::RDGTopology::MakeShadowCSR();
  tsuba::RDGTopology* csr = KATANA_CHECKED_CONTEXT(
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
        rdg.node_entity_type_id_array_file_storage(),
        rdg.IsUint16tEntityTypeIDs()));

    EntityTypeIDArray edge_type_ids = KATANA_CHECKED(MapEntityTypeIDsArray(
        rdg.edge_entity_type_id_array_file_storage(),
        rdg.IsUint16tEntityTypeIDs()));

    KATANA_ASSERT(topo.num_nodes() == node_type_ids.size());
    KATANA_ASSERT(topo.num_edges() == edge_type_ids.size());

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
        MakeDefaultEntityTypeIDArray(topo.num_nodes()),
        MakeDefaultEntityTypeIDArray(topo.num_edges()), EntityTypeManager{},
        EntityTypeManager{});

    KATANA_CHECKED(pg->ConstructEntityTypeIDs());

    return MakeResult(std::move(pg));
  }
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    const std::string& rdg_name, const tsuba::RDGLoadOptions& opts) {
  tsuba::RDGManifest manifest = KATANA_CHECKED(tsuba::FindManifest(rdg_name));
  tsuba::RDGFile rdg_file{
      KATANA_CHECKED(tsuba::Open(std::move(manifest), tsuba::kReadWrite))};
  tsuba::RDG rdg = KATANA_CHECKED(tsuba::RDG::Make(rdg_file, opts));

  return katana::PropertyGraph::Make(
      std::make_unique<tsuba::RDGFile>(std::move(rdg_file)), std::move(rdg));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    const tsuba::RDGManifest& rdg_manifest, const tsuba::RDGLoadOptions& opts) {
  tsuba::RDGFile rdg_file{
      KATANA_CHECKED(tsuba::Open(std::move(rdg_manifest), tsuba::kReadWrite))};
  tsuba::RDG rdg = KATANA_CHECKED(tsuba::RDG::Make(rdg_file, opts));

  return katana::PropertyGraph::Make(
      std::make_unique<tsuba::RDGFile>(std::move(rdg_file)), std::move(rdg));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(katana::GraphTopology&& topo_to_assign) {
  return std::make_unique<katana::PropertyGraph>(
      std::unique_ptr<tsuba::RDGFile>(), tsuba::RDG{},
      std::move(topo_to_assign),
      MakeDefaultEntityTypeIDArray(topo_to_assign.num_nodes()),
      MakeDefaultEntityTypeIDArray(topo_to_assign.num_edges()),
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
      std::unique_ptr<tsuba::RDGFile>(), tsuba::RDG{},
      std::move(topo_to_assign), std::move(node_entity_type_ids),
      std::move(edge_entity_type_ids), std::move(node_type_manager),
      std::move(edge_type_manager));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Copy() const {
  return Copy(
      loaded_node_schema()->field_names(), loaded_edge_schema()->field_names());
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Copy(
    const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) const {
  // TODO(gill): This should copy the RDG in memory without reloading from storage.
  tsuba::RDGLoadOptions opts;
  opts.partition_id_to_load = partition_id();
  opts.node_properties = node_properties;
  opts.edge_properties = edge_properties;

  return Make(rdg_dir(), opts);
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
      static_cast<uint64_t>(rdg_.node_properties()->num_rows());
  if (num_node_rows == 0) {
    if ((rdg_.node_properties()->num_columns() != 0) && (num_nodes() != 0)) {
      return KATANA_ERROR(
          ErrorCode::AssertionFailed,
          "number of rows in node properties is 0 but "
          "the number of node properties is {} and the number of nodes is {}",
          rdg_.node_properties()->num_columns(), num_nodes());
    }
  } else if (num_node_rows != num_nodes()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "number of rows in node properties {} differs "
        "from the number of nodes {}",
        rdg_.node_properties()->num_rows(), num_nodes());
  }

  if (num_nodes() != node_entity_type_ids_.size()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "Number of nodes {} differs"
        "from the number of node IDs {} in the node type set ID array",
        num_nodes(), node_entity_type_ids_.size());
  }

  if (num_edges() != edge_entity_type_ids_.size()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "Number of edges {} differs"
        "from the number of edge IDs {} in the edge type set ID array",
        num_edges(), edge_entity_type_ids_.size());
  }

  uint64_t num_edge_rows =
      static_cast<uint64_t>(rdg_.edge_properties()->num_rows());
  if (num_edge_rows == 0) {
    if ((rdg_.edge_properties()->num_columns() != 0) && (num_edges() != 0)) {
      return KATANA_ERROR(
          ErrorCode::AssertionFailed,
          "number of rows in edge properties is 0 but "
          "the number of edge properties is {} and the number of edges is {}",
          rdg_.edge_properties()->num_columns(), num_edges());
    }
  } else if (num_edge_rows != num_edges()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "number of rows in edge properties {} differs "
        "from the number of edges {}",
        rdg_.edge_properties()->num_rows(), num_edges());
  }

  return katana::ResultSuccess();
}

/// Converts all uint8/bool properties into EntityTypeIDs
/// Only call this if every uint8/bool property should be considered a type
katana::Result<void>
katana::PropertyGraph::ConstructEntityTypeIDs() {
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
  node_entity_type_manager_ = EntityTypeManager{};
  node_entity_type_ids_ = EntityTypeIDArray{};
  node_entity_type_ids_.allocateInterleaved(num_nodes());
  auto node_props_to_remove =
      KATANA_CHECKED(EntityTypeManager::AssignEntityTypeIDsFromProperties(
          num_nodes(), rdg_.node_properties(), &node_entity_type_manager_,
          &node_entity_type_ids_));
  for (const auto& node_prop : node_props_to_remove) {
    KATANA_CHECKED(RemoveNodeProperty(node_prop));
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
  edge_entity_type_manager_ = EntityTypeManager{};
  edge_entity_type_ids_ = EntityTypeIDArray{};
  edge_entity_type_ids_.allocateInterleaved(num_edges());
  auto edge_props_to_remove =
      KATANA_CHECKED(EntityTypeManager::AssignEntityTypeIDsFromProperties(
          num_edges(), rdg_.edge_properties(), &edge_entity_type_manager_,
          &edge_entity_type_ids_));
  for (const auto& edge_prop : edge_props_to_remove) {
    KATANA_CHECKED(RemoveEdgeProperty(edge_prop));
  }

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::DoWriteTopologies() {
  // Since PGViewCache doesn't manage the main csr topology, see if we need to store it now
  tsuba::RDGTopology shadow = KATANA_CHECKED(tsuba::RDGTopology::Make(
      topology().adj_data(), topology().num_nodes(), topology().dest_data(),
      topology().num_edges(), tsuba::RDGTopology::TopologyKind::kCSR,
      tsuba::RDGTopology::TransposeKind::kNo,
      tsuba::RDGTopology::EdgeSortKind::kAny,
      tsuba::RDGTopology::NodeSortKind::kAny));

  rdg_.UpsertTopology(std::move(shadow));

  std::vector<tsuba::RDGTopology> topologies =
      KATANA_CHECKED(pg_view_cache_.ToRDGTopology());
  for (size_t i = 0; i < topologies.size(); i++) {
    rdg_.UpsertTopology(std::move(topologies.at(i)));
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::DoWrite(
    tsuba::RDGHandle handle, const std::string& command_line,
    tsuba::RDG::RDGVersioningPolicy versioning_action) {
  KATANA_LOG_DEBUG(
      " node array valid: {}, edge array valid: {}",
      rdg_.node_entity_type_id_array_file_storage().Valid(),
      rdg_.edge_entity_type_id_array_file_storage().Valid());

  KATANA_CHECKED(DoWriteTopologies());

  if (!rdg_.node_entity_type_id_array_file_storage().Valid()) {
    KATANA_LOG_DEBUG("node_entity_type_id_array file store invalid, writing");
  }

  std::unique_ptr<tsuba::FileFrame> node_entity_type_id_array_res =
      !rdg_.node_entity_type_id_array_file_storage().Valid() ||
              !rdg_.IsUint16tEntityTypeIDs()
          ? KATANA_CHECKED(WriteEntityTypeIDsArray(node_entity_type_ids_))
          : nullptr;

  if (!rdg_.edge_entity_type_id_array_file_storage().Valid()) {
    KATANA_LOG_DEBUG("edge_entity_type_id_array file store invalid, writing");
  }

  std::unique_ptr<tsuba::FileFrame> edge_entity_type_id_array_res =
      !rdg_.edge_entity_type_id_array_file_storage().Valid() ||
              !rdg_.IsUint16tEntityTypeIDs()
          ? KATANA_CHECKED(WriteEntityTypeIDsArray(edge_entity_type_ids_))
          : nullptr;

  return rdg_.Store(
      handle, command_line, versioning_action,
      std::move(node_entity_type_id_array_res),
      std::move(edge_entity_type_id_array_res), node_entity_type_manager(),
      edge_entity_type_manager());
}

katana::Result<void>
katana::PropertyGraph::ConductWriteOp(
    const std::string& uri, const std::string& command_line,
    tsuba::RDG::RDGVersioningPolicy versioning_action) {
  tsuba::RDGManifest manifest = KATANA_CHECKED(tsuba::FindManifest(uri));
  tsuba::RDGHandle rdg_handle =
      KATANA_CHECKED(tsuba::Open(std::move(manifest), tsuba::kReadWrite));
  auto new_file = std::make_unique<tsuba::RDGFile>(rdg_handle);

  if (auto res = DoWrite(*new_file, command_line, versioning_action); !res) {
    return res.error();
  }

  file_ = std::move(new_file);

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::WriteView(
    const std::string& uri, const std::string& command_line) {
  return ConductWriteOp(
      uri, command_line, tsuba::RDG::RDGVersioningPolicy::RetainVersion);
}

katana::Result<void>
katana::PropertyGraph::WriteGraph(
    const std::string& uri, const std::string& command_line) {
  return ConductWriteOp(
      uri, command_line, tsuba::RDG::RDGVersioningPolicy::IncrementVersion);
}

katana::Result<void>
katana::PropertyGraph::Commit(const std::string& command_line) {
  if (file_ == nullptr) {
    if (rdg_.rdg_dir().empty()) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument, "RDG commit but rdg_dir_ is empty");
    }
    return WriteGraph(rdg_.rdg_dir().string(), command_line);
  }
  return DoWrite(
      *file_, command_line, tsuba::RDG::RDGVersioningPolicy::IncrementVersion);
}

katana::Result<void>
katana::PropertyGraph::WriteView(const std::string& command_line) {
  // WriteView occurs once, and only before any Commit/Write operation
  KATANA_LOG_DEBUG_ASSERT(file_ == nullptr);
  return WriteView(rdg_.rdg_dir().string(), command_line);
}

bool
katana::PropertyGraph::Equals(const PropertyGraph* other) const {
  if (!topology().Equals(other->topology())) {
    return false;
  }

  if (!node_entity_type_manager_.Equals(other->node_entity_type_manager())) {
    return false;
  }

  if (!edge_entity_type_manager_.Equals(other->edge_entity_type_manager())) {
    return false;
  }

  // The TypeIDs can change, but their string interpretation cannot
  if (node_entity_type_ids_.size() != other->node_entity_type_ids_.size()) {
    return false;
  }
  for (size_t i = 0; i < node_entity_type_ids_.size(); ++i) {
    auto tns = node_entity_type_manager_.EntityTypeToTypeNameSet(
        node_entity_type_ids_[i]);
    auto otns = other->node_entity_type_manager_.EntityTypeToTypeNameSet(
        other->node_entity_type_ids_[i]);
    if (tns != otns) {
      return false;
    }
  }

  // The TypeIDs can change, but their string interpretation cannot
  if (edge_entity_type_ids_.size() != other->edge_entity_type_ids_.size()) {
    return false;
  }
  for (size_t i = 0; i < edge_entity_type_ids_.size(); ++i) {
    auto tns = edge_entity_type_manager_.EntityTypeToTypeNameSet(
        edge_entity_type_ids_[i]);
    auto otns = other->edge_entity_type_manager_.EntityTypeToTypeNameSet(
        other->edge_entity_type_ids_[i]);
    if (tns != otns) {
      return false;
    }
  }

  const auto& node_props = rdg_.node_properties();
  const auto& edge_props = rdg_.edge_properties();
  const auto& other_node_props = other->rdg_.node_properties();
  const auto& other_edge_props = other->rdg_.edge_properties();
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
        topology().num_nodes(), topology().num_edges(),
        other->topology().num_nodes(), other->topology().num_edges());
  } else {
    fmt::format_to(std::back_inserter(buf), "Topologies match!\n");
  }

  fmt::format_to(std::back_inserter(buf), "NodeEntityTypeManager Diff:\n");
  fmt::format_to(
      std::back_inserter(buf),
      node_entity_type_manager_.ReportDiff(other->node_entity_type_manager()));
  fmt::format_to(std::back_inserter(buf), "EdgeEntityTypeManager Diff:\n");
  fmt::format_to(
      std::back_inserter(buf),
      edge_entity_type_manager_.ReportDiff(other->edge_entity_type_manager()));

  // The TypeIDs can change, but their string interpretation cannot
  bool match = true;
  if (node_entity_type_ids_.size() != other->node_entity_type_ids_.size()) {
    fmt::format_to(
        std::back_inserter(buf),
        "node_entity_type_ids differ. size {} vs. {}\n",
        node_entity_type_ids_size(), other->node_entity_type_ids_size());
    match = false;
  } else {
    for (size_t i = 0; i < node_entity_type_ids_.size(); ++i) {
      auto tns_res = node_entity_type_manager_.EntityTypeToTypeNameSet(
          node_entity_type_ids_[i]);
      auto otns_res = other->node_entity_type_manager_.EntityTypeToTypeNameSet(
          other->node_entity_type_ids_[i]);
      if (!tns_res || !otns_res) {
        fmt::format_to(
            std::back_inserter(buf),
            "node error types index {} entity lhs {} entity rhs_{}\n", i,
            node_entity_type_ids_[i], other->node_entity_type_ids_[i]);
        match = false;
        break;
      }
      auto tns = tns_res.value();
      auto otns = otns_res.value();
      if (tns != otns) {
        fmt::format_to(
            std::back_inserter(buf),
            "node_entity_type_ids differ. {:4} {} {} {} {}\n", i,
            node_entity_type_ids_[i], fmt::join(tns, ", "),
            other->node_entity_type_ids_[i], fmt::join(otns, ", "));
        match = false;
      }
    }
  }
  if (match) {
    fmt::format_to(std::back_inserter(buf), "node_entity_type_ids Match!\n");
  }

  // The TypeIDs can change, but their string interpretation cannot
  match = true;
  if (edge_entity_type_ids_.size() != other->edge_entity_type_ids_.size()) {
    fmt::format_to(
        std::back_inserter(buf),
        "edge_entity_type_ids differ. size {} vs. {}\n",
        edge_entity_type_ids_size(), other->edge_entity_type_ids_size());
    match = false;
  } else {
    for (size_t i = 0; i < edge_entity_type_ids_.size(); ++i) {
      auto tns_res = edge_entity_type_manager_.EntityTypeToTypeNameSet(
          edge_entity_type_ids_[i]);
      auto otns_res = other->edge_entity_type_manager_.EntityTypeToTypeNameSet(
          other->edge_entity_type_ids_[i]);
      if (!tns_res || !otns_res) {
        fmt::format_to(
            std::back_inserter(buf),
            "edge error types index {} entity lhs {} entity rhs_{}\n", i,
            edge_entity_type_ids_[i], other->edge_entity_type_ids_[i]);
        match = false;
        break;
      }
      auto tns = tns_res.value();
      auto otns = otns_res.value();
      if (tns != otns) {
        fmt::format_to(
            std::back_inserter(buf),
            "edge_entity_type_ids differ. {:4} {} {} {} {}\n", i,
            edge_entity_type_ids_[i], fmt::join(tns, ", "),
            other->edge_entity_type_ids_[i], fmt::join(otns, ", "));
        match = false;
      }
    }
  }
  if (match) {
    fmt::format_to(std::back_inserter(buf), "edge_entity_type_ids Match!\n");
  }

  const auto& node_props = rdg_.node_properties();
  const auto& edge_props = rdg_.edge_properties();
  const auto& other_node_props = other->rdg_.node_properties();
  const auto& other_edge_props = other->rdg_.edge_properties();
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
  auto ret = rdg_.node_properties()->GetColumnByName(name);
  if (ret) {
    return MakeResult(std::move(ret));
  }
  return KATANA_ERROR(
      ErrorCode::PropertyNotFound, "node property does not exist: {}", name);
}

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
katana::PropertyGraph::GetEdgeProperty(const std::string& name) const {
  auto ret = rdg_.edge_properties()->GetColumnByName(name);
  if (ret) {
    return MakeResult(std::move(ret));
  }
  return KATANA_ERROR(
      ErrorCode::PropertyNotFound, "edge property does not exist: {}", name);
}

katana::Result<void>
katana::PropertyGraph::Write(
    const std::string& rdg_name, const std::string& command_line) {
  if (auto res = tsuba::Create(rdg_name); !res) {
    return res.error();
  }
  return WriteGraph(rdg_name, command_line);
}

katana::Result<void>
katana::PropertyGraph::AddNodeProperties(
    const std::shared_ptr<arrow::Table>& props) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("adding empty node prop table");
    return ResultSuccess();
  }
  if (topology().num_nodes() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().num_nodes(), props->num_rows());
  }
  return rdg_.AddNodeProperties(props);
}

katana::Result<void>
katana::PropertyGraph::UpsertNodeProperties(
    const std::shared_ptr<arrow::Table>& props, tsuba::TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("upsert empty node prop table");
    return ResultSuccess();
  }
  if (topology().num_nodes() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().num_nodes(), props->num_rows());
  }
  return rdg_.UpsertNodeProperties(props, txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::RemoveNodeProperty(int i) {
  return rdg_.RemoveNodeProperty(i);
}

katana::Result<void>
katana::PropertyGraph::RemoveNodeProperty(const std::string& prop_name) {
  auto col_names = rdg_.node_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
  if (pos != col_names.cend()) {
    return rdg_.RemoveNodeProperty(std::distance(col_names.cbegin(), pos));
  }
  return katana::ErrorCode::PropertyNotFound;
}

katana::Result<void>
katana::PropertyGraph::LoadNodeProperty(const std::string& name, int i) {
  return rdg_.LoadNodeProperty(name, i);
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

std::vector<std::string>
katana::PropertyGraph::ListNodeProperties() const {
  return rdg_.ListNodeProperties();
}

std::vector<std::string>
katana::PropertyGraph::ListEdgeProperties() const {
  return rdg_.ListEdgeProperties();
}

katana::Result<void>
katana::PropertyGraph::UnloadNodeProperty(const std::string& prop_name) {
  return rdg_.UnloadNodeProperty(prop_name);
}

katana::Result<void>
katana::PropertyGraph::AddEdgeProperties(
    const std::shared_ptr<arrow::Table>& props) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("adding empty edge prop table");
    return ResultSuccess();
  }
  if (topology().num_edges() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().num_edges(), props->num_rows());
  }
  return rdg_.AddEdgeProperties(props);
}

katana::Result<void>
katana::PropertyGraph::UpsertEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, tsuba::TxnContext* txn_ctx) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("upsert empty edge prop table");
    return ResultSuccess();
  }
  if (topology().num_edges() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().num_edges(), props->num_rows());
  }
  return rdg_.UpsertEdgeProperties(props, txn_ctx);
}

katana::Result<void>
katana::PropertyGraph::RemoveEdgeProperty(int i) {
  return rdg_.RemoveEdgeProperty(i);
}

katana::Result<void>
katana::PropertyGraph::RemoveEdgeProperty(const std::string& prop_name) {
  auto col_names = rdg_.edge_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
  if (pos != col_names.cend()) {
    return rdg_.RemoveEdgeProperty(std::distance(col_names.cbegin(), pos));
  }
  return katana::ErrorCode::PropertyNotFound;
}

katana::Result<void>
katana::PropertyGraph::UnloadEdgeProperty(const std::string& prop_name) {
  return rdg_.UnloadEdgeProperty(prop_name);
}

katana::Result<void>
katana::PropertyGraph::LoadEdgeProperty(const std::string& name, int i) {
  return rdg_.LoadEdgeProperty(name, i);
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
katana::PropertyGraph::MakeNodeIndex(const std::string& column_name) {
  for (const auto& existing_index : node_indexes_) {
    if (existing_index->column_name() == column_name) {
      return KATANA_ERROR(
          katana::ErrorCode::AlreadyExists,
          "Index already exists for column {}", column_name);
    }
  }

  // Get a view of the property.
  std::shared_ptr<arrow::ChunkedArray> chunked_property =
      KATANA_CHECKED(GetNodeProperty(column_name));
  KATANA_LOG_ASSERT(chunked_property->num_chunks() == 1);
  std::shared_ptr<arrow::Array> property = chunked_property->chunk(0);

  // Create an index based on the type of the field.
  std::unique_ptr<katana::PropertyIndex<GraphTopology::Node>> index =
      KATANA_CHECKED(katana::MakeTypedIndex<katana::GraphTopology::Node>(
          column_name, num_nodes(), property));

  KATANA_CHECKED(index->BuildFromProperty());

  node_indexes_.push_back(std::move(index));

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::DeleteNodeIndex(const std::string& column_name) {
  for (auto it = node_indexes_.begin(); it != node_indexes_.end(); it++) {
    if ((*it)->column_name() == column_name) {
      node_indexes_.erase(it);
      return katana::ResultSuccess();
    }
  }
  return KATANA_ERROR(katana::ErrorCode::NotFound, "node index not found");
}

// Build an index over edges.
katana::Result<void>
katana::PropertyGraph::MakeEdgeIndex(const std::string& column_name) {
  for (const auto& existing_index : edge_indexes_) {
    if (existing_index->column_name() == column_name) {
      return KATANA_ERROR(
          katana::ErrorCode::AlreadyExists,
          "Index already exists for column {}", column_name);
    }
  }

  // Get a view of the property.
  std::shared_ptr<arrow::ChunkedArray> chunked_property =
      KATANA_CHECKED(GetEdgeProperty(column_name));
  KATANA_LOG_ASSERT(chunked_property->num_chunks() == 1);
  std::shared_ptr<arrow::Array> property = chunked_property->chunk(0);

  // Create an index based on the type of the field.
  std::unique_ptr<katana::PropertyIndex<katana::GraphTopology::Edge>> index =
      KATANA_CHECKED(katana::MakeTypedIndex<katana::GraphTopology::Edge>(
          column_name, num_edges(), property));

  KATANA_CHECKED(index->BuildFromProperty());

  edge_indexes_.push_back(std::move(index));

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::DeleteEdgeIndex(const std::string& column_name) {
  for (auto it = edge_indexes_.begin(); it != edge_indexes_.end(); it++) {
    if ((*it)->column_name() == column_name) {
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
  permutation_vec->allocateInterleaved(topo.num_edges());
  katana::ParallelSTL::iota(
      permutation_vec->begin(), permutation_vec->end(), uint64_t{0});

  auto* out_dests_data = const_cast<GraphTopology::Node*>(topo.dest_data());

  katana::do_all(
      katana::iterate(pg->topology().all_nodes()),
      [&](GraphTopology::Node n) {
        const auto e_beg = *pg->topology().edges(n).begin();
        const auto e_end = *pg->topology().edges(n).end();

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
  auto e_range = topo.edges(src);

  constexpr size_t kBinarySearchThreshold = 64;

  if (e_range.size() <= kBinarySearchThreshold) {
    auto iter = std::find_if(
        e_range.begin(), e_range.end(),
        [&](const GraphTopology::Edge& e) { return topo.edge_dest(e) == dst; });

    return *iter;

  } else {
    auto iter = std::lower_bound(
        e_range.begin(), e_range.end(), dst,
        internal::EdgeDestComparator<GraphTopology>{&topo});

    return topo.edge_dest(*iter) == dst ? *iter : *e_range.end();
  }
}

// TODO(amber): this method should return a new sorted topology
katana::Result<void>
katana::SortNodesByDegree(katana::PropertyGraph* pg) {
  const auto& topo = pg->topology();

  uint64_t num_nodes = topo.num_nodes();
  uint64_t num_edges = topo.num_edges();

  using DegreeNodePair = std::pair<uint64_t, uint32_t>;
  katana::NUMAArray<DegreeNodePair> dn_pairs;
  dn_pairs.allocateInterleaved(num_nodes);

  katana::do_all(katana::iterate(topo.all_nodes()), [&](auto node) {
    size_t node_degree = pg->edges(node).size();
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

  auto* out_dests_data = const_cast<GraphTopology::Node*>(topo.dest_data());
  auto* out_indices_data = const_cast<GraphTopology::Edge*>(topo.adj_data());

  katana::do_all(
      katana::iterate(topo.all_nodes()),
      [&](auto old_node_id) {
        uint32_t new_node_id = old_to_new_mapping[old_node_id];

        // get the start location of this reindex'd nodes edges
        uint64_t new_out_index =
            (new_node_id == 0) ? 0 : new_prefix_sum[new_node_id - 1];

        // construct the graph, reindexing as it goes along
        for (auto e : topo.edges(old_node_id)) {
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
  if (topology.num_nodes() == 0) {
    return std::make_unique<PropertyGraph>();
  }

  // New symmetric graph topology
  katana::NUMAArray<uint64_t> out_indices;
  katana::NUMAArray<uint32_t> out_dests;

  out_indices.allocateInterleaved(topology.num_nodes());
  // Store the out-degree of nodes from original graph
  katana::do_all(katana::iterate(topology.all_nodes()), [&](auto n) {
    out_indices[n] = topology.edges(n).size();
  });

  katana::do_all(
      katana::iterate(topology.all_nodes()),
      [&](auto n) {
        // update the out_indices for the symmetric topology
        for (auto e : topology.edges(n)) {
          auto dest = topology.edge_dest(e);
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

  uint64_t num_nodes_symmetric = topology.num_nodes();
  uint64_t num_edges_symmetric = out_indices[num_nodes_symmetric - 1];

  katana::NUMAArray<uint64_t> out_dests_offset;
  out_dests_offset.allocateInterleaved(topology.num_nodes());
  // Temp NUMAArray for computing new destination positions
  out_dests_offset[0] = 0;
  katana::do_all(
      katana::iterate(uint64_t{1}, topology.num_nodes()),
      [&](uint64_t n) { out_dests_offset[n] = out_indices[n - 1]; },
      katana::no_stats());

  out_dests.allocateInterleaved(num_edges_symmetric);
  // Update graph topology with the original edges + reverse edges
  katana::do_all(
      katana::iterate(topology.all_nodes()),
      [&](auto src) {
        // get all outgoing edges (excluding self edges) of a particular
        // node and add reverse edges.
        for (GraphTopology::Edge e : topology.edges(src)) {
          // e = start index into edge array for a particular node
          // destination node
          auto dest = topology.edge_dest(e);

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
  if (topology.num_nodes() == 0) {
    return std::make_unique<PropertyGraph>();
  }

  katana::NUMAArray<GraphTopology::Edge> out_indices;
  katana::NUMAArray<GraphTopology::Node> out_dests;

  out_indices.allocateInterleaved(topology.num_nodes());
  out_dests.allocateInterleaved(topology.num_edges());

  // Initialize the new topology indices
  katana::do_all(
      katana::iterate(uint64_t{0}, topology.num_nodes()),
      [&](uint64_t n) { out_indices[n] = uint64_t{0}; }, katana::no_stats());

  // Keep a copy of old destinaton ids and compute number of
  // in-coming edges for the new prefix sum of out_indices.
  katana::do_all(
      katana::iterate(topology.all_edges()),
      [&](auto e) {
        // Counting outgoing edges in the tranpose graph by
        // counting incoming edges in the original graph
        auto dest = topology.edge_dest(e);
        __sync_add_and_fetch(&(out_indices[dest]), 1);
      },
      katana::no_stats());

  // Prefix sum calculation of the edge index array
  katana::ParallelSTL::partial_sum(
      out_indices.begin(), out_indices.end(), out_indices.begin());

  katana::NUMAArray<uint64_t> out_dests_offset;
  out_dests_offset.allocateInterleaved(topology.num_nodes());

  // temporary buffer for storing the starting point of each node's transpose
  // adjacency
  out_dests_offset[0] = 0;
  katana::do_all(
      katana::iterate(uint64_t{1}, topology.num_nodes()),
      [&](uint64_t n) { out_dests_offset[n] = out_indices[n - 1]; },
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
        }
      },
      katana::no_stats());

  GraphTopology transpose_topo{std::move(out_indices), std::move(out_dests)};
  return katana::PropertyGraph::Make(std::move(transpose_topo));
}

katana::Result<katana::PropertyIndex<katana::GraphTopology::Node>*>
katana::PropertyGraph::GetNodePropertyIndex(
    const std::string& property_name) const {
  for (const auto& index : node_indexes()) {
    if (index->column_name() == property_name) {
      return index.get();
    }
  }
  return KATANA_ERROR(katana::ErrorCode::NotFound, "node index not found");
}

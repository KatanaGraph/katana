#include "tsuba/RDGSlice.h"

#include "AddProperties.h"
#include "RDGCore.h"
#include "RDGHandleImpl.h"
#include "katana/ArrowInterchange.h"
#include "katana/EntityTypeManager.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/RDGPrefix.h"
#include "tsuba/RDGTopology.h"

katana::Result<void>
tsuba::RDGSlice::DoMake(
    const std::optional<std::vector<std::string>>& node_props,
    const std::optional<std::vector<std::string>>& edge_props,
    const katana::Uri& metadata_dir, const SliceArg& slice) {
  slice_arg_ = slice;

  ReadGroup grp;

  KATANA_CHECKED(core_->MakeTopologyManager(metadata_dir));

  // must have csr topology to Make an RDGSlice
  tsuba::RDGTopology shadow = tsuba::RDGTopology::MakeShadowCSR();
  tsuba::RDGTopology* topo =
      KATANA_CHECKED(core_->topology_manager().GetTopology(shadow));

  KATANA_CHECKED_CONTEXT(
      topo->Bind(
          metadata_dir, slice.topo_off, slice.topo_off + slice.topo_size, true),
      "loading topology array; begin: {}, end: {}", slice.topo_off,
      slice.topo_off + slice.topo_size);

  if (core_->part_header().IsEntityTypeIDsOutsideProperties()) {
    katana::Uri node_types_path = metadata_dir.Join(
        core_->part_header().node_entity_type_id_array_path());
    katana::Uri edge_types_path = metadata_dir.Join(
        core_->part_header().edge_entity_type_id_array_path());

    // NB: we add sizeof(EntityTypeIDArrayHeader) to every range element because
    // the structure of this file is
    // [header, value, value, value, ...]
    // it would be nice if RDGCore could handle this format complication, but
    // the uses are different enough between RDG and RDGSlice that it probably
    // doesn't make sense

    size_t storage_entity_type_id_size = 0;
    if (core_->part_header().IsUint16tEntityTypeIDs()) {
      storage_entity_type_id_size = sizeof(katana::EntityTypeID);
    } else {
      storage_entity_type_id_size = sizeof(uint8_t);
    }

    KATANA_CHECKED_CONTEXT(
        core_->node_entity_type_id_array_file_storage().Bind(
            node_types_path.string(),
            sizeof(EntityTypeIDArrayHeader) +
                slice.node_range.first * storage_entity_type_id_size,
            sizeof(EntityTypeIDArrayHeader) +
                slice.node_range.second * storage_entity_type_id_size,
            true),
        "loading node type id array; begin: {}, end: {}",
        slice.node_range.first * storage_entity_type_id_size,
        slice.node_range.second * storage_entity_type_id_size);
    KATANA_CHECKED_CONTEXT(
        core_->edge_entity_type_id_array_file_storage().Bind(
            edge_types_path.string(),
            sizeof(EntityTypeIDArrayHeader) +
                slice.edge_range.first * storage_entity_type_id_size,
            sizeof(EntityTypeIDArrayHeader) +
                slice.edge_range.second * storage_entity_type_id_size,
            true),
        "loading edge type id array; begin: {}, end: {}",
        slice.edge_range.first * storage_entity_type_id_size,
        slice.edge_range.second * storage_entity_type_id_size);
  }
  core_->set_rdg_dir(metadata_dir);
  // all of the properties
  std::vector<PropStorageInfo*> node_properties =
      KATANA_CHECKED(core_->part_header().SelectNodeProperties(node_props));

  KATANA_CHECKED(AddPropertySlice(
      metadata_dir, node_properties, slice.node_range, &grp,
      [rdg = this](
          const std::shared_ptr<arrow::Table>& props) -> katana::Result<void> {
        std::shared_ptr<arrow::Table> prop_table =
            rdg->core_->node_properties();

        if (prop_table && prop_table->num_columns() > 0) {
          for (int i = 0; i < props->num_columns(); ++i) {
            prop_table = KATANA_CHECKED(prop_table->AddColumn(
                prop_table->num_columns(), props->field(i), props->column(i)));
          }
        } else {
          prop_table = props;
        }
        rdg->core_->set_node_properties(std::move(prop_table));
        return katana::ResultSuccess();
      }));

  // all of the properties
  std::vector<PropStorageInfo*> edge_properties =
      KATANA_CHECKED(core_->part_header().SelectEdgeProperties(edge_props));

  KATANA_CHECKED(AddPropertySlice(
      metadata_dir, edge_properties, slice.edge_range, &grp,
      [rdg = this](
          const std::shared_ptr<arrow::Table>& props) -> katana::Result<void> {
        std::shared_ptr<arrow::Table> prop_table =
            rdg->core_->edge_properties();

        if (prop_table && prop_table->num_columns() > 0) {
          for (int i = 0; i < props->num_columns(); ++i) {
            prop_table = KATANA_CHECKED(prop_table->AddColumn(
                prop_table->num_columns(), props->field(i), props->column(i)));
          }
        } else {
          prop_table = props;
        }
        rdg->core_->set_edge_properties(std::move(prop_table));
        return katana::ResultSuccess();
      }));
  core_->set_rdg_dir(metadata_dir);

  // check if there are any properties left to load
  // any properties left at this point will really be partition metadata arrays
  // (which we load via the property interface)
  std::vector<PropStorageInfo*> part_info =
      KATANA_CHECKED(core_->part_header().SelectPartitionProperties());

  // these are not Node/Edge types but rather property types we are checking
  KATANA_CHECKED(core_->EnsureNodeTypesLoaded());
  KATANA_CHECKED(core_->EnsureEdgeTypesLoaded());

  if (part_info.empty()) {
    return grp.Finish();
  }

  // metadata arrays that should be loaded now (as oppposed to on-demand)
  std::vector<PropStorageInfo*> load_now;

  for (PropStorageInfo* prop : part_info) {
    std::string name = prop->name();
    if (name == RDGCore::kMasterNodesPropName ||
        name == RDGCore::kMirrorNodesPropName ||
        name == RDGCore::kHostToOwnedGlobalNodeIDsPropName ||
        name == RDGCore::kHostToOwnedGlobalEdgeIDsPropName) {
      load_now.push_back(prop);
    }
  }

  KATANA_CHECKED_CONTEXT(
      AddProperties(
          metadata_dir, tsuba::NodeEdge::kNeitherNodeNorEdge, nullptr, nullptr,
          load_now, &grp,
          [rdg = this](const std::shared_ptr<arrow::Table>& props) {
            return rdg->core_->AddPartitionMetadataArray(props);
          }),
      "populating partition metadata");
  KATANA_CHECKED(grp.Finish());

  return katana::ResultSuccess();
}

katana::Result<tsuba::RDGSlice>
tsuba::RDGSlice::Make(
    RDGHandle handle, const SliceArg& slice, const uint32_t partition_id,
    const std::optional<std::vector<std::string>>& node_props,
    const std::optional<std::vector<std::string>>& edge_props) {
  const RDGManifest& manifest = handle.impl_->rdg_manifest();
  katana::Uri partition_path(manifest.PartitionFileName(partition_id));

  auto part_header = KATANA_CHECKED(RDGPartHeader::Make(partition_path));

  RDGSlice rdg_slice(std::make_unique<RDGCore>(std::move(part_header)));

  KATANA_CHECKED(
      rdg_slice.DoMake(node_props, edge_props, manifest.dir(), slice));

  return RDGSlice(std::move(rdg_slice));
}

katana::Result<std::pair<std::vector<size_t>, std::vector<size_t>>>
tsuba::RDGSlice::GetPerPartitionCounts(RDGHandle handle) {
  katana::Uri part_0_part_file =
      handle.impl_->rdg_manifest().PartitionFileName(0);
  auto part_0_header = KATANA_CHECKED_CONTEXT(
      RDGPartHeader::Make(part_0_part_file),
      "getting part header for partition 0");

  KATANA_LOG_ASSERT(handle.impl_->rdg_manifest().num_hosts());
  std::vector<size_t> num_nodes_per_host(
      handle.impl_->rdg_manifest().num_hosts());
  std::vector<size_t> num_edges_per_host(
      handle.impl_->rdg_manifest().num_hosts());
  katana::Uri dir = handle.impl_->rdg_manifest().dir();
  std::vector<PropStorageInfo*> part_props = KATANA_CHECKED_CONTEXT(
      part_0_header.SelectPartitionProperties(),
      "getting partition metadata property storage locations");
  for (PropStorageInfo* prop : part_props) {
    if (prop->name() == RDGCore::kHostToOwnedGlobalNodeIDsPropName) {
      katana::Uri path = dir.Join(prop->path());
      auto nodes_table = KATANA_CHECKED_CONTEXT(
          LoadProperties(prop->name(), path),
          "getting host to owned nodes for per host node count");
      auto host_to_owned_nodes = KATANA_CHECKED_CONTEXT(
          katana::UnmarshalVector<uint64_t>(nodes_table->column(0)),
          "converting host to owned nodes arrow array to vector");
      if (num_nodes_per_host.size() != host_to_owned_nodes.size()) {
        return KATANA_ERROR(
            tsuba::ErrorCode::PropertyNotFound,
            "host to owned node array on storage had unexpected size: {} "
            "(expected {})",
            host_to_owned_nodes.size(), num_nodes_per_host.size());
      }
      num_nodes_per_host[0] = host_to_owned_nodes[0];
      for (size_t i = 1, size = num_nodes_per_host.size(); i < size; ++i) {
        num_nodes_per_host[i] =
            host_to_owned_nodes[i] - host_to_owned_nodes[i - 1];
      }
    }
    if (prop->name() == RDGCore::kHostToOwnedGlobalEdgeIDsPropName) {
      katana::Uri path = dir.Join(prop->path());
      auto edges_table = KATANA_CHECKED_CONTEXT(
          LoadProperties(prop->name(), path),
          "getting host to owned edges for per host edge count");
      auto host_to_owned_edges = KATANA_CHECKED_CONTEXT(
          katana::UnmarshalVector<uint64_t>(edges_table->column(0)),
          "converting host to owned edges arrow array to vector");
      if (num_edges_per_host.size() != host_to_owned_edges.size()) {
        return KATANA_ERROR(
            tsuba::ErrorCode::PropertyNotFound,
            "host to owned edge array on storage had unexpected size: {} "
            "(expected {})",
            host_to_owned_edges.size(), num_edges_per_host.size());
      }
      num_edges_per_host[0] = host_to_owned_edges[0];
      for (size_t i = 1, size = num_edges_per_host.size(); i < size; ++i) {
        num_edges_per_host[i] =
            host_to_owned_edges[i] - host_to_owned_edges[i - 1];
      }
    }
  }

  return {num_nodes_per_host, num_edges_per_host};
}

const katana::Uri&
tsuba::RDGSlice::rdg_dir() const {
  return core_->rdg_dir();
}

uint32_t
tsuba::RDGSlice::partition_id() const {
  return core_->partition_id();
}

const std::vector<std::shared_ptr<arrow::ChunkedArray>>&
tsuba::RDGSlice::master_nodes() const {
  return core_->master_nodes();
}

const std::vector<std::shared_ptr<arrow::ChunkedArray>>&
tsuba::RDGSlice::mirror_nodes() const {
  return core_->mirror_nodes();
}

const std::shared_ptr<arrow::ChunkedArray>&
tsuba::RDGSlice::host_to_owned_global_node_ids() const {
  return core_->host_to_owned_global_node_ids();
}

const std::shared_ptr<arrow::ChunkedArray>&
tsuba::RDGSlice::host_to_owned_global_edge_ids() const {
  return core_->host_to_owned_global_edge_ids();
}

const std::shared_ptr<arrow::ChunkedArray>&
tsuba::RDGSlice::local_to_user_id() const {
  return core_->local_to_user_id();
}

const std::shared_ptr<arrow::ChunkedArray>&
tsuba::RDGSlice::local_to_global_id() const {
  return core_->local_to_global_id();
}

katana::Result<void>
tsuba::RDGSlice::load_local_to_global_id() {
  auto ltg_it = std::find_if(
      core_->part_header().part_prop_info_list().begin(),
      core_->part_header().part_prop_info_list().end(),
      [](PropStorageInfo& psi) {
        return psi.name() == RDGCore::kLocalToGlobalIDPropName;
      });
  if (ltg_it == core_->part_header().part_prop_info_list().end()) {
    // some RDGs have no local to global array - use the default 0-length array
    // instead
    core_->set_local_to_global_id(katana::NullChunkedArray(arrow::uint64(), 0));
    return katana::ResultSuccess();
  }
  std::vector<PropStorageInfo*> local_to_global{&(*ltg_it)};
  KATANA_CHECKED_CONTEXT(
      AddProperties(
          core_->rdg_dir(), tsuba::NodeEdge::kNeitherNodeNorEdge, nullptr,
          nullptr, local_to_global, nullptr,
          [slice = this](const std::shared_ptr<arrow::Table>& props) {
            return slice->core_->AddPartitionMetadataArray(props);
          }),
      "populating local to global id");
  return katana::ResultSuccess();
}
katana::Result<void>
tsuba::RDGSlice::load_local_to_user_id() {
  auto ltu_it = std::find_if(
      core_->part_header().part_prop_info_list().begin(),
      core_->part_header().part_prop_info_list().end(),
      [](PropStorageInfo& psi) {
        return psi.name() == RDGCore::kLocalToUserIDPropName;
      });
  if (ltu_it == core_->part_header().part_prop_info_list().end()) {
    // some RDGs have no local to user array - use the default 0-length array
    // instead
    core_->set_local_to_user_id(katana::NullChunkedArray(arrow::uint64(), 0));
    return katana::ResultSuccess();
  }
  std::vector<PropStorageInfo*> local_to_user{&(*ltu_it)};
  KATANA_CHECKED_CONTEXT(
      AddProperties(
          core_->rdg_dir(), tsuba::NodeEdge::kNeitherNodeNorEdge, nullptr,
          nullptr, local_to_user, nullptr,
          [slice = this](const std::shared_ptr<arrow::Table>& props) {
            return slice->core_->AddPartitionMetadataArray(props);
          }),
      "populating local to global id");
  return katana::ResultSuccess();
}
katana::Result<void>
tsuba::RDGSlice::remove_local_to_global_id() {
  auto ltg_it = std::find_if(
      core_->part_header().part_prop_info_list().begin(),
      core_->part_header().part_prop_info_list().end(),
      [](PropStorageInfo& psi) {
        return psi.name() == RDGCore::kLocalToGlobalIDPropName;
      });
  if (ltg_it != core_->part_header().part_prop_info_list().end()) {
    ltg_it->WasUnloaded();
  }
  core_->set_local_to_global_id(katana::NullChunkedArray(arrow::uint64(), 0));
  return katana::ResultSuccess();
}
katana::Result<void>
tsuba::RDGSlice::remove_local_to_user_id() {
  auto ltu_it = std::find_if(
      core_->part_header().part_prop_info_list().begin(),
      core_->part_header().part_prop_info_list().end(),
      [](PropStorageInfo& psi) {
        return psi.name() == RDGCore::kLocalToUserIDPropName;
      });
  if (ltu_it != core_->part_header().part_prop_info_list().end()) {
    ltu_it->WasUnloaded();
  }
  core_->set_local_to_user_id(katana::NullChunkedArray(arrow::uint64(), 0));
  return katana::ResultSuccess();
}

const std::shared_ptr<arrow::Table>&
tsuba::RDGSlice::node_properties() const {
  return core_->node_properties();
}

const std::shared_ptr<arrow::Table>&
tsuba::RDGSlice::edge_properties() const {
  return core_->edge_properties();
}

const tsuba::FileView&
tsuba::RDGSlice::topology_file_storage() const {
  tsuba::RDGTopology shadow = tsuba::RDGTopology::MakeShadowCSR();
  auto res = core_->topology_manager().GetTopology(shadow);
  KATANA_LOG_VASSERT(res, "CSR topology is no longer available");

  tsuba::RDGTopology* topo = res.value();

  KATANA_LOG_VASSERT(topo->bound(), "CSR topology file store is not bound");
  return topo->file_storage();
}

const tsuba::FileView&
tsuba::RDGSlice::node_entity_type_id_array_file_storage() const {
  return core_->node_entity_type_id_array_file_storage();
}

const tsuba::FileView&
tsuba::RDGSlice::edge_entity_type_id_array_file_storage() const {
  return core_->edge_entity_type_id_array_file_storage();
}

katana::Result<katana::EntityTypeManager>
tsuba::RDGSlice::node_entity_type_manager() const {
  return core_->part_header().GetNodeEntityTypeManager();
}

katana::Result<katana::EntityTypeManager>
tsuba::RDGSlice::edge_entity_type_manager() const {
  return core_->part_header().GetEdgeEntityTypeManager();
}

tsuba::RDGSlice::RDGSlice(std::unique_ptr<RDGCore>&& core)
    : core_(std::move(core)) {}

tsuba::RDGSlice::~RDGSlice() = default;
tsuba::RDGSlice::RDGSlice(RDGSlice&& other) noexcept = default;
tsuba::RDGSlice& tsuba::RDGSlice::operator=(RDGSlice&& other) noexcept =
    default;

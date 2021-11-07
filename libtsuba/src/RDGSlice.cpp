#include "tsuba/RDGSlice.h"

#include "AddProperties.h"
#include "RDGCore.h"
#include "RDGHandleImpl.h"
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
  ReadGroup grp;

  KATANA_CHECKED_CONTEXT(
      core_->MakeTopologyManager(metadata_dir), "populating topologies");

  tsuba::RDGTopology shadow = tsuba::RDGTopology::MakeShadowCSR();
  tsuba::RDGTopology* topo = KATANA_CHECKED_CONTEXT(
      core_->topology_manager().GetTopology(shadow),
      "unable to find csr topology, must have csr topology to Make an "
      "RDGSlice");

  KATANA_CHECKED_CONTEXT(
      topo->Bind(
          metadata_dir, slice.topo_off, slice.topo_off + slice.topo_size, true),
      "loading topology array");

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
        slice.node_range.first * sizeof(katana::EntityTypeID),
        slice.node_range.second * sizeof(katana::EntityTypeID));
    KATANA_CHECKED_CONTEXT(
        core_->edge_entity_type_id_array_file_storage().Bind(
            edge_types_path.string(),
            sizeof(EntityTypeIDArrayHeader) +
                slice.edge_range.first * storage_entity_type_id_size,
            sizeof(EntityTypeIDArrayHeader) +
                slice.edge_range.second * storage_entity_type_id_size,
            true),
        "loading edge type id array");
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

  KATANA_CHECKED_CONTEXT(
      AddPropertySlice(
          metadata_dir, edge_properties, slice.edge_range, &grp,
          [rdg = this](const std::shared_ptr<arrow::Table>& props)
              -> katana::Result<void> {
            std::shared_ptr<arrow::Table> prop_table =
                rdg->core_->edge_properties();

            if (prop_table && prop_table->num_columns() > 0) {
              for (int i = 0; i < props->num_columns(); ++i) {
                prop_table = KATANA_CHECKED(prop_table->AddColumn(
                    prop_table->num_columns(), props->field(i),
                    props->column(i)));
              }
            } else {
              prop_table = props;
            }
            rdg->core_->set_edge_properties(std::move(prop_table));
            return katana::ResultSuccess();
          }),
      "populating edge properties");
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

  // metadata arrays that should be loaded unsliced, in their entirety
  std::vector<PropStorageInfo*> no_slice;
  // metadata arrays that should have their slice range adjusted for the fact
  // that they only have entries for mirror nodes
  std::vector<PropStorageInfo*> no_masters;
  uint32_t num_masters = core_->part_header().metadata().num_owned_;
  std::pair<uint64_t, uint64_t> no_masters_range = {
      slice.node_range.first >= num_masters
          ? slice.node_range.first - num_masters
          : 0,
      slice.node_range.second >= num_masters
          ? slice.node_range.second - num_masters
          : 0};
  // metadata arrays that should be loaded with the same node slice range as
  // regular node properties
  std::vector<PropStorageInfo*> node_slicing;
  std::pair<uint64_t, uint64_t> node_slicing_range = slice.node_range;

  for (PropStorageInfo* prop : part_info) {
    std::string name = prop->name();
    if (name == RDGCore::kMasterNodesPropName ||
        name == RDGCore::kMirrorNodesPropName ||
        name == RDGCore::kHostToOwnedGlobalNodeIDsPropName ||
        name == RDGCore::kHostToOwnedGlobalEdgeIDsPropName) {
      no_slice.push_back(prop);
    } else if (name == RDGCore::kLocalToGlobalIDPropName) {
      no_masters.push_back(prop);
    } else if (name == RDGCore::kLocalToUserIDPropName) {
      node_slicing.push_back(prop);
    }
  }

  KATANA_CHECKED_CONTEXT(
      AddProperties(
          metadata_dir, tsuba::NodeEdge::kNeitherNodeNorEdge, nullptr, nullptr,
          no_slice, &grp,
          [rdg = this](const std::shared_ptr<arrow::Table>& props) {
            return rdg->core_->AddPartitionMetadataArray(props);
          }),
      "populating partition metadata");
  KATANA_CHECKED_CONTEXT(
      AddPropertySlice(
          metadata_dir, no_masters, no_masters_range, &grp,
          [rdg = this](const std::shared_ptr<arrow::Table>& props) {
            return rdg->core_->AddPartitionMetadataArray(props);
          }),
      "populating partition metadata");
  KATANA_CHECKED_CONTEXT(
      AddPropertySlice(
          metadata_dir, node_slicing, node_slicing_range, &grp,
          [rdg = this](const std::shared_ptr<arrow::Table>& props) {
            return rdg->core_->AddPartitionMetadataArray(props);
          }),
      "populating partition metadata");
  KATANA_CHECKED(grp.Finish());

  return katana::ResultSuccess();
}

katana::Result<tsuba::RDGSlice>
tsuba::RDGSlice::Make(
    RDGHandle handle, const SliceArg& slice,
    const std::optional<std::vector<std::string>>& node_props,
    const std::optional<std::vector<std::string>>& edge_props) {
  const RDGManifest& manifest = handle.impl_->rdg_manifest();
  if (manifest.num_hosts() != 1) {
    return KATANA_ERROR(
        ErrorCode::NotImplemented,
        "cannot construct RDGSlice for partitioned graph");
  }
  katana::Uri partition_path(manifest.PartitionFileName(0));

  auto part_header = KATANA_CHECKED(RDGPartHeader::Make(partition_path));

  RDGSlice rdg_slice(std::make_unique<RDGCore>(std::move(part_header)));

  if (auto res =
          rdg_slice.DoMake(node_props, edge_props, manifest.dir(), slice);
      !res) {
    return res.error();
  }

  return RDGSlice(std::move(rdg_slice));
}

const katana::Uri&
tsuba::RDGSlice::rdg_dir() const {
  return core_->rdg_dir();
}

uint32_t
tsuba::RDGSlice::partition_id() const {
  return core_->partition_id();
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

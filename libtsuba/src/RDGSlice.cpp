#include "tsuba/RDGSlice.h"

#include "AddProperties.h"
#include "RDGCore.h"
#include "RDGHandleImpl.h"
#include "katana/EntityTypeManager.h"
#include "katana/Logging.h"
#include "tsuba/Errors.h"

katana::Result<void>
tsuba::RDGSlice::DoMake(
    const std::optional<std::vector<std::string>>& node_props,
    const std::optional<std::vector<std::string>>& edge_props,
    const katana::Uri& metadata_dir, const SliceArg& slice) {
  ReadGroup grp;
  katana::Uri topology_path =
      metadata_dir.Join(core_->part_header().topology_path());
  KATANA_CHECKED_CONTEXT(
      core_->topology_file_storage().Bind(
          topology_path.string(), slice.topo_off,
          slice.topo_off + slice.topo_size, true),
      "loading topology array");

  if (core_->part_header().IsEntityTypeIDsOutsideProperties()) {
    katana::Uri node_types_path = metadata_dir.Join(
        core_->part_header().node_entity_type_id_array_path());
    katana::Uri edge_types_path = metadata_dir.Join(
        core_->part_header().edge_entity_type_id_array_path());

    KATANA_CHECKED_CONTEXT(
        core_->node_entity_type_id_array_file_storage().Bind(
            node_types_path.string(),
            slice.node_range.first * sizeof(katana::EntityTypeID),
            slice.node_range.second * sizeof(katana::EntityTypeID), true),
        "loading node type id array; begin: {}, end: {}",
        slice.node_range.first * sizeof(katana::EntityTypeID),
        slice.node_range.second * sizeof(katana::EntityTypeID));
    KATANA_CHECKED_CONTEXT(
        core_->edge_entity_type_id_array_file_storage().Bind(
            edge_types_path.string(),
            slice.edge_range.first * sizeof(katana::EntityTypeID),
            slice.edge_range.second * sizeof(katana::EntityTypeID), true),
        "loading edge type id array");
  }
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

  auto edge_result = AddPropertySlice(
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
      });
  if (!edge_result) {
    return edge_result.error();
  }

  return grp.Finish();
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
  return core_->topology_file_storage();
}

const tsuba::FileView&
tsuba::RDGSlice::node_entity_type_id_array_file_storage() const {
  return core_->node_entity_type_id_array_file_storage();
}

const tsuba::FileView&
tsuba::RDGSlice::edge_entity_type_id_array_file_storage() const {
  return core_->edge_entity_type_id_array_file_storage();
}

tsuba::RDGSlice::RDGSlice(std::unique_ptr<RDGCore>&& core)
    : core_(std::move(core)) {}

tsuba::RDGSlice::~RDGSlice() = default;
tsuba::RDGSlice::RDGSlice(RDGSlice&& other) noexcept = default;
tsuba::RDGSlice& tsuba::RDGSlice::operator=(RDGSlice&& other) noexcept =
    default;

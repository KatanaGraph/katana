#include "tsuba/RDGSlice.h"

#include "AddProperties.h"
#include "RDGCore.h"
#include "RDGHandleImpl.h"
#include "katana/Logging.h"
#include "tsuba/Errors.h"

katana::Result<void>
tsuba::RDGSlice::DoMake(
    const std::optional<std::vector<std::string>>& node_props,
    const std::optional<std::vector<std::string>>& edge_props,
    const katana::Uri& metadata_dir, const SliceArg& slice) {
  ReadGroup grp;
  katana::Uri t_path = metadata_dir.Join(core_->part_header().topology_path());

  if (auto res = core_->topology_file_storage().Bind(
          t_path.string(), slice.topo_off, slice.topo_off + slice.topo_size,
          true);
      !res) {
    return res.error();
  }

  // all of the properties
  std::vector<PropStorageInfo*> node_properties =
      KATANA_CHECKED(core_->part_header().SelectNodeProperties(node_props));

  KATANA_CHECKED(AddPropertySlice(
      metadata_dir, node_properties, slice.node_range, &grp,
      [rdg = this](const std::shared_ptr<arrow::Table>& props) {
        return rdg->core_->AddNodeProperties(props);
      }));

  // all of the properties
  std::vector<PropStorageInfo*> edge_properties =
      KATANA_CHECKED(core_->part_header().SelectEdgeProperties(edge_props));

  auto edge_result = AddPropertySlice(
      metadata_dir, edge_properties, slice.edge_range, &grp,
      [rdg = this](const std::shared_ptr<arrow::Table>& props) {
        return rdg->core_->AddEdgeProperties(props);
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

tsuba::RDGSlice::RDGSlice(std::unique_ptr<RDGCore>&& core)
    : core_(std::move(core)) {}

tsuba::RDGSlice::~RDGSlice() = default;
tsuba::RDGSlice::RDGSlice(RDGSlice&& other) noexcept = default;
tsuba::RDGSlice& tsuba::RDGSlice::operator=(RDGSlice&& other) noexcept =
    default;

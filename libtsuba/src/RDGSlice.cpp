#include "tsuba/RDGSlice.h"

#include "AddProperties.h"
#include "RDGCore.h"
#include "RDGHandleImpl.h"
#include "katana/Logging.h"
#include "tsuba/Errors.h"

katana::Result<void>
tsuba::RDGSlice::DoMake(
    const katana::Uri& metadata_dir, const SliceArg& slice) {
  ReadGroup grp;
  katana::Uri t_path = metadata_dir.Join(core_->part_header().topology_path());

  if (auto res = core_->topology_file_storage().Bind(
          t_path.string(), slice.topo_off, slice.topo_off + slice.topo_size,
          true);
      !res) {
    return res.error();
  }

  auto node_result = AddPropertySlice(
      metadata_dir, core_->part_header().node_prop_info_list(),
      slice.node_range, &grp,
      [rdg = this](const std::shared_ptr<arrow::Table>& props) {
        return rdg->core_->AddNodeProperties(props);
      });
  if (!node_result) {
    return node_result.error();
  }

  auto edge_result = AddPropertySlice(
      metadata_dir, core_->part_header().edge_prop_info_list(),
      slice.edge_range, &grp,
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
    const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props) {
  const RDGMeta& meta = handle.impl_->rdg_meta();
  if (meta.num_hosts() != 1) {
    return KATANA_ERROR(
        ErrorCode::NotImplemented,
        "cannot construct RDGSlice for partitioned graph");
  }
  katana::Uri partition_path(meta.PartitionFileName(0));

  auto part_header_res = RDGPartHeader::Make(partition_path);
  if (!part_header_res) {
    return part_header_res.error().WithContext(
        "error reading metadata from {}", partition_path);
  }

  RDGSlice rdg_slice(
      std::make_unique<RDGCore>(std::move(part_header_res.value())));

  if (auto res =
          rdg_slice.core_->part_header().PrunePropsTo(node_props, edge_props);
      !res) {
    return res.error();
  }

  if (auto res = rdg_slice.DoMake(meta.dir(), slice); !res) {
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

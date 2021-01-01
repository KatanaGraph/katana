#include "tsuba/RDGSlice.h"

#include "AddTables.h"
#include "RDGCore.h"
#include "RDGHandleImpl.h"
#include "katana/Logging.h"
#include "tsuba/Errors.h"

namespace tsuba {

katana::Result<void>
RDGSlice::DoMake(const katana::Uri& metadata_dir, const SliceArg& slice) {
  katana::Uri t_path = metadata_dir.Join(core_->part_header().topology_path());

  if (auto res = core_->topology_file_storage().Bind(
          t_path.string(), slice.topo_off, slice.topo_off + slice.topo_size,
          true);
      !res) {
    return res.error();
  }

  auto node_result = AddTablesSlice(
      metadata_dir, core_->part_header().node_prop_info_list(),
      slice.node_range,
      [rdg = this](const std::shared_ptr<arrow::Table>& table) {
        return rdg->core_->AddNodeProperties(table);
      });
  if (!node_result) {
    return node_result.error();
  }

  auto edge_result = AddTablesSlice(
      metadata_dir, core_->part_header().edge_prop_info_list(),
      slice.edge_range,
      [rdg = this](const std::shared_ptr<arrow::Table>& table) {
        return rdg->core_->AddEdgeProperties(table);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  return katana::ResultSuccess();
}

katana::Result<RDGSlice>
RDGSlice::Make(
    RDGHandle handle, const SliceArg& slice,
    const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props) {
  const RDGMeta& meta = handle.impl_->rdg_meta();
  if (meta.num_hosts() != 1) {
    KATANA_LOG_ERROR("cannot construct RDGSlice for partitioned graph");
    return ErrorCode::NotImplemented;
  }
  katana::Uri partition_path(meta.PartitionFileName(0));

  auto part_header_res = RDGPartHeader::Make(partition_path);
  if (!part_header_res) {
    KATANA_LOG_DEBUG(
        "failed: ReadMetaData (path: {}): {}", partition_path,
        part_header_res.error());
    return part_header_res.error();
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
RDGSlice::node_table() const {
  return core_->node_table();
}

const std::shared_ptr<arrow::Table>&
RDGSlice::edge_table() const {
  return core_->edge_table();
}

const FileView&
RDGSlice::topology_file_storage() const {
  return core_->topology_file_storage();
}

RDGSlice::RDGSlice(std::unique_ptr<RDGCore>&& core) : core_(std::move(core)) {}

RDGSlice::~RDGSlice() = default;
RDGSlice::RDGSlice(RDGSlice&& other) noexcept = default;
RDGSlice& RDGSlice::operator=(RDGSlice&& other) noexcept = default;

}  // namespace tsuba

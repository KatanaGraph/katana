#include "tsuba/RDGSlice.h"

#include "AddTables.h"
#include "RDGHandleImpl.h"
#include "galois/Logging.h"
#include "tsuba/Errors.h"

namespace tsuba {

galois::Result<void>
RDGSlice::DoLoad(const galois::Uri& metadata_dir, const SliceArg& slice) {
  galois::Uri t_path = metadata_dir.Join(core_.part_header().topology_path());

  if (auto res = core_.topology_file_storage().Bind(
          t_path.string(), slice.topo_off, slice.topo_off + slice.topo_size,
          true);
      !res) {
    return res.error();
  }

  auto node_result = AddTablesSlice(
      metadata_dir, core_.part_header().node_prop_info_list(), slice.node_range,
      [rdg = this](const std::shared_ptr<arrow::Table>& table) {
        return rdg->core_.AddNodeProperties(table);
      });
  if (!node_result) {
    return node_result.error();
  }

  auto edge_result = AddTablesSlice(
      metadata_dir, core_.part_header().edge_prop_info_list(), slice.edge_range,
      [rdg = this](const std::shared_ptr<arrow::Table>& table) {
        return rdg->core_.AddEdgeProperties(table);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  return galois::ResultSuccess();
}

galois::Result<RDGSlice>
RDGSlice::Make(
    const RDGMeta& meta, const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props, const SliceArg& slice) {
  auto name_res = meta.PartitionFileName(true);
  if (!name_res) {
    GALOIS_LOG_DEBUG(
        "failed: getting name of partition header {}", name_res.error());
    return name_res.error();
  }
  galois::Uri partition_path(std::move(name_res.value()));

  auto part_header_res = RDGPartHeader::Make(partition_path);
  if (!part_header_res) {
    GALOIS_LOG_DEBUG(
        "failed: ReadMetaData (path: {}): {}", partition_path,
        part_header_res.error());
    return part_header_res.error();
  }

  RDGSlice rdg_slice(std::move(part_header_res.value()));

  if (auto res =
          rdg_slice.core_.part_header().PrunePropsTo(node_props, edge_props);
      !res) {
    return res.error();
  }

  if (auto res = rdg_slice.DoLoad(meta.dir(), slice); !res) {
    return res.error();
  }

  return RDGSlice(std::move(rdg_slice));
}

galois::Result<RDGSlice>
RDGSlice::Load(
    RDGHandle handle, const SliceArg& slice,
    const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props) {
  if (!handle.impl_->AllowsReadPartial()) {
    GALOIS_LOG_DEBUG("failed: handle does not allow partial read");
    return ErrorCode::InvalidArgument;
  }
  return Make(handle.impl_->rdg_meta, node_props, edge_props, slice);
}

galois::Result<RDGSlice>
RDGSlice::Load(
    const std::string& rdg_meta_path, const SliceArg& slice,
    const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props) {
  auto uri_res = galois::Uri::Make(rdg_meta_path);
  if (!uri_res) {
    return uri_res.error();
  }
  auto meta_res = tsuba::RDGMeta::Make(uri_res.value());
  if (!meta_res) {
    return meta_res.error();
  }
  return Make(meta_res.value(), node_props, edge_props, slice);
}

}  // namespace tsuba

#include "tsuba/RDGPrefix.h"

#include "RDGHandleImpl.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/RDGPartHeader.h"
#include "tsuba/file.h"

namespace tsuba {

galois::Result<tsuba::RDGPrefix>
RDGPrefix::DoMakePrefix(const tsuba::RDGMeta& meta) {
  auto part_file_res = meta.PartitionFileName(true);
  if (!part_file_res) {
    return part_file_res.error();
  }
  auto meta_res = RDGPartHeader::Make(part_file_res.value());
  if (!meta_res) {
    return meta_res.error();
  }

  tsuba::RDGPartHeader part_header = std::move(meta_res.value());
  if (part_header.topology_path().empty()) {
    return RDGPrefix{};
  }

  galois::Uri t_path = meta.dir().Join(part_header.topology_path());

  RDGPrefix::GRHeader gr_header;
  if (auto res = FileGet(t_path.string(), &gr_header); !res) {
    GALOIS_LOG_DEBUG(
        "file get failed: {}: sz: {}: {}", t_path, sizeof(gr_header),
        res.error());
    return res.error();
  }
  FileView fv;
  if (auto res = fv.Bind(
          t_path.string(),
          sizeof(gr_header) + (gr_header.num_nodes * sizeof(uint64_t)), true);
      !res) {
    GALOIS_LOG_DEBUG("fileview bind failed: {}: {}", t_path, res.error());
    return res.error();
  }

  return RDGPrefix(
      std::move(fv),
      sizeof(gr_header) + (gr_header.num_nodes * sizeof(uint64_t)));
}

galois::Result<tsuba::RDGPrefix>
RDGPrefix::Make(const std::string& uri_str) {
  auto uri_res = galois::Uri::Make(uri_str);
  if (!uri_res) {
    return uri_res.error();
  }
  auto meta_res = RDGMeta::Make(uri_res.value());
  if (!meta_res) {
    return meta_res.error();
  }
  return DoMakePrefix(meta_res.value());
}

galois::Result<tsuba::RDGPrefix>
RDGPrefix::Make(RDGHandle handle) {
  if (!handle.impl_->AllowsReadPartial()) {
    GALOIS_LOG_DEBUG("failed: handle not intended for partial read");
    return ErrorCode::InvalidArgument;
  }
  return DoMakePrefix(handle.impl_->rdg_meta);
}

}  // namespace tsuba

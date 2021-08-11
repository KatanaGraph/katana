#include "tsuba/RDGPrefix.h"

#include "RDGHandleImpl.h"
#include "RDGManifest.h"
#include "RDGPartHeader.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

namespace tsuba {

katana::Result<tsuba::RDGPrefix>
RDGPrefix::DoMakePrefix(const tsuba::RDGManifest& manifest) {
  katana::RDGVersion version = manifest.version();

  auto meta_res = RDGPartHeader::Make(manifest.PartitionFileName(0));
  if (!meta_res) {
    return meta_res.error();
  }

  tsuba::RDGPartHeader part_header = std::move(meta_res.value());
  if (part_header.topology_path().empty()) {
    return RDGPrefix{};
  }

  katana::Uri t_path = manifest.dir().Join(part_header.topology_path());

  CSRTopologyHeader gr_header;
  if (auto res = FileGet(t_path.string(), &gr_header); !res) {
    return res.error().WithContext(
        "file get failed: {}: sz: {}", t_path, sizeof(gr_header));
  }
  FileView fv;
  if (auto res = fv.Bind(
          t_path.string(),
          sizeof(gr_header) + (gr_header.num_nodes * sizeof(uint64_t)), true);
      !res) {
    return res.error().WithContext("failed to bind {}", t_path);
  }

  return RDGPrefix(
      std::move(fv),
      sizeof(gr_header) + (gr_header.num_nodes * sizeof(uint64_t)));
}

katana::Result<tsuba::RDGPrefix>
RDGPrefix::Make(RDGHandle handle) {
  if (handle.impl_->rdg_manifest().num_hosts() != 1) {
    return KATANA_ERROR(
        ErrorCode::NotImplemented,
        "cannot construct RDGPrefix for partitioned graph");
  }

  return DoMakePrefix(handle.impl_->rdg_manifest());
}

}  // namespace tsuba

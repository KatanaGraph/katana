#include "tsuba/RDGPrefix.h"

#include "RDGHandleImpl.h"
#include "RDGPartHeader.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/RDGManifest.h"
#include "tsuba/file.h"

namespace tsuba {

katana::Result<tsuba::RDGPrefix>
RDGPrefix::DoMakePrefix(const tsuba::RDGManifest& manifest) {
  auto part_header =
      KATANA_CHECKED(RDGPartHeader::Make(manifest.PartitionFileName(0)));

  if (part_header.csr_topology_path().empty()) {
    return RDGPrefix{};
  }

  katana::Uri t_path = manifest.dir().Join(part_header.csr_topology_path());

  CSRTopologyHeader gr_header;
  KATANA_CHECKED_CONTEXT(
      FileGet(t_path.string(), &gr_header), "file get failed: {}: sz: {}",
      t_path, sizeof(gr_header));

  FileView fv;
  KATANA_CHECKED_CONTEXT(
      fv.Bind(
          t_path.string(),
          sizeof(gr_header) + (gr_header.num_nodes * sizeof(uint64_t)), true),
      "failed to bind {}", t_path);

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

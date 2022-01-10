#include "katana/RDGPrefix.h"

#include "RDGHandleImpl.h"
#include "RDGPartHeader.h"
#include "katana/ErrorCode.h"
#include "katana/RDGManifest.h"
#include "katana/Result.h"
#include "katana/file.h"

katana::Result<katana::RDGPrefix>
katana::RDGPrefix::DoMakePrefix(
    const katana::RDGManifest& manifest, uint32_t partition_id) {
  auto part_header = KATANA_CHECKED(
      RDGPartHeader::Make(manifest.PartitionFileName(partition_id)));

  if (part_header.csr_topology_path().empty()) {
    return RDGPrefix{};
  }

  katana::Uri t_path = manifest.dir().Join(part_header.csr_topology_path());

  CSRTopologyHeader gr_header;
  KATANA_CHECKED_CONTEXT(
      FileGet(t_path.string(), &gr_header), "file get failed: {}; sz: {}",
      t_path, sizeof(gr_header));

  FileView fv;
  uint64_t offset =
      sizeof(gr_header) + (gr_header.num_nodes * sizeof(uint64_t));
  KATANA_CHECKED_CONTEXT(
      fv.Bind(t_path.string(), offset, true),
      "failed to bind {}; begin: 0, end: {}", t_path, offset);

  return RDGPrefix(std::move(fv), offset);
}

katana::Result<katana::RDGPrefix>
katana::RDGPrefix::Make(katana::RDGHandle handle, uint32_t partition_id) {
  return DoMakePrefix(handle.impl_->rdg_manifest(), partition_id);
}

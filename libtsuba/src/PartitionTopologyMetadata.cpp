#include "PartitionTopologyMetadata.h"

#include <string>

#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/RDGTopology.h"
#include "tsuba/tsuba.h"

namespace tsuba {

PartitionTopologyMetadataEntry*
PartitionTopologyMetadata::GetEntry(uint32_t index) {
  KATANA_LOG_ASSERT(index < num_entries_);
  return &(entries_.at(index));
}

PartitionTopologyMetadataEntry*
PartitionTopologyMetadata::Append(PartitionTopologyMetadataEntry entry) {
  KATANA_LOG_VASSERT(
      num_entries_ < kMaxNumTopologies,
      "cannot add more than kMaxNumTopologies entries");
  entries_[num_entries_] = std::move(entry);

  num_entries_++;
  return &(entries_.at(num_entries_ - 1));
}

katana::Result<void>
PartitionTopologyMetadata::Validate() const {
  // basic validation, a CSR topology must be present
  if (entries_.empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "no topology metadata entries present");
  }

  bool csr_topo_found = false;
  for (size_t i = 0; i < num_entries_; i++) {
    if (entries_.at(i).topology_state_ == RDGTopology::TopologyKind::kCSR) {
      csr_topo_found = true;
    }
    KATANA_CHECKED(ValidateEntry(entries_.at(i)));
  }

  if (!csr_topo_found) {
    return KATANA_ERROR(ErrorCode::InvalidArgument, "no csr topology present");
  }

  return katana::ResultSuccess();
}

std::string
PartitionTopologyMetadata::EntryToString(
    const PartitionTopologyMetadataEntry& entry) const {
  return fmt::format(
      "transpose_{}_node_sort_{}_edge_sort_{}_num_nodes_{}_num_edges_{}",
      entry.transpose_state_, entry.node_sort_state_, entry.edge_sort_state_,
      entry.num_nodes_, entry.num_edges_);
}

katana::Result<void>
PartitionTopologyMetadata::ValidateEntry(
    const PartitionTopologyMetadataEntry& entry) const {
  if (entry.path_.empty()) {
    std::string name = EntryToString(entry);
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "no topology file path: topology: {} ",
        name);
  }
  if (entry.path_.find('/') != std::string::npos) {
    std::string name = EntryToString(entry);
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "topology path must not contain a leading (/): path = {}, "
        "topology: {}",
        entry.path_, name);
  }
  return katana::ResultSuccess();
}

}  // namespace tsuba

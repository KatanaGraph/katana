#include "RDGTopologyManager.h"

#include <cstddef>
#include <memory>

#include "PartitionTopologyMetadata.h"
#include "RDGPartHeader.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/RDGTopology.h"
#include "tsuba/tsuba.h"

namespace tsuba {

katana::Result<RDGTopology*>
RDGTopologyManager::GetTopology(const RDGTopology& shadow) {
  // find if we have a toplogy that matches the shadow

  KATANA_LOG_DEBUG(
      "checking for topology with: topology_state={}, transpose_state={}, "
      "edge_sort_state={}, node_sort_state={}",
      shadow.topology_state(), shadow.transpose_state(),
      shadow.edge_sort_state(), shadow.node_sort_state());

  for (size_t i = 0; i < num_topologies_; i++) {
    if (shadow.topology_state() == topology_set_.at(i).topology_state() &&
        (shadow.transpose_state() == topology_set_.at(i).transpose_state() ||
         shadow.transpose_state() == RDGTopology::TransposeKind::kAny) &&
        shadow.edge_sort_state() == topology_set_.at(i).edge_sort_state() &&
        shadow.node_sort_state() == topology_set_.at(i).node_sort_state() &&
        !topology_set_.at(i).invalid()) {
      KATANA_LOG_DEBUG(
          "Found topology matching shadow, num_topologies_ = {}",
          num_topologies_);
      return &(topology_set_.at(i));
    }
  }

  KATANA_LOG_DEBUG(
      "Unable to locate topology matching shadow, num_topologies_ = {}",
      num_topologies_);
  return KATANA_ERROR(ErrorCode::InvalidArgument, "No matching topology found");
}

katana::Result<tsuba::RDGTopologyManager>
RDGTopologyManager::Make(PartitionTopologyMetadata* topology_metadata) {
  RDGTopologyManager manager = RDGTopologyManager();

  for (size_t i = 0; i < topology_metadata->num_entries(); i++) {
    RDGTopology topology =
        KATANA_CHECKED(RDGTopology::Make(topology_metadata->GetEntry(i)));
    manager.Append(std::move(topology));
  }
  return RDGTopologyManager(std::move(manager));
}

katana::Result<void>
RDGTopologyManager::DoStore(
    RDGHandle handle, std::unique_ptr<tsuba::WriteGroup>& write_group) {
  KATANA_LOG_VASSERT(num_topologies_ >= 1, "must have at least 1 topology");
  for (size_t i = 0; i < num_topologies_; i++) {
    // don't store invalid RDGTopology instances, they have been superseded
    if (topology_set_.at(i).invalid()) {
      continue;
    }

    KATANA_LOG_VASSERT(
        topology_set_.at(i).metadata_entry_valid(),
        "topology at index {} must have valid metadata before calling DoStore",
        i);
    KATANA_CHECKED(topology_set_.at(i).DoStore(handle, write_group));
  }
  return katana::ResultSuccess();
}

}  // namespace tsuba

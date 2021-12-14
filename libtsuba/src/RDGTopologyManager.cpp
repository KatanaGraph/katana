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
    RDGHandle handle, const katana::Uri& current_rdg_dir,
    std::unique_ptr<tsuba::WriteGroup>& write_group) {
  KATANA_LOG_VASSERT(num_topologies_ >= 1, "must have at least 1 topology");
  KATANA_LOG_DEBUG("Storing {} RDGTopologies", num_topologies_);

  for (size_t i = 0; i < num_topologies_; i++) {
    // Ensure that all RDGTopologies get unbound before we get to storing
    // Keeping the file bound is unnecessary and is a huge waste of memory
    // since GraphTopology copys the data out of the RDGTopology file and into
    // its own arrays
    KATANA_LOG_VASSERT(
        !topology_set_.at(i).bound() && !topology_set_.at(i).mapped(),
        "All RDGTopologies should be unbound ");

    // don't store invalid RDGTopology instances, they have been superseded
    if (topology_set_.at(i).invalid()) {
      continue;
    }

    KATANA_LOG_VASSERT(
        topology_set_.at(i).metadata_entry_valid(),
        "topology at index {} must have valid metadata before calling DoStore",
        i);
    KATANA_CHECKED(
        topology_set_.at(i).DoStore(handle, current_rdg_dir, write_group));
  }
  return katana::ResultSuccess();
}

}  // namespace tsuba

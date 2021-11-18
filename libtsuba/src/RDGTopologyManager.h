#ifndef KATANA_LIBTSUBA_RDGTOPOLOGYMANAGER_H_
#define KATANA_LIBTSUBA_RDGTOPOLOGYMANAGER_H_

#include <cstddef>

#include "PartitionTopologyMetadata.h"
#include "RDGPartHeader.h"
#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "tsuba/Errors.h"
#include "tsuba/RDGTopology.h"
#include "tsuba/tsuba.h"

namespace tsuba {

///store references to the various topologies, provide functionality to map various topologies
class KATANA_EXPORT RDGTopologyManager {
public:
  RDGTopologyManager() = default;

  /// takes a topology shadow struct/class, finds the topology with matching flags

  katana::Result<RDGTopology*> GetTopology(const RDGTopology& shadow);

  /// update or insert an RDGTopology
  void Upsert(RDGTopology topo) {
    auto res = GetTopology(topo);
    if (res) {
      // we already have a topology matching this one. Mark the existing topology as invalid so it is not stored,
      // and add the new topology to the manager.
      RDGTopology* existing_topo = res.value();
      existing_topo->set_invalid();
    }

    Append(std::move(topo));
    return;
  }

  /// add a RDGTopology to the manager
  void Append(RDGTopology topo) {
    KATANA_LOG_VASSERT(
        !GetTopology(topo), "cannot append an identical RDGTopology");

    KATANA_LOG_VASSERT(
        topo.metadata_entry_valid(),
        "cannot append entry with invalid metadata entry");
    KATANA_LOG_VASSERT(
        num_topologies_ < kMaxNumTopologies,
        "cannot add more than kMaxNumTopologies entries");
    topology_set_[num_topologies_] = std::move(topo);
    num_topologies_++;
  }

  katana::Result<void> DoStore(
      RDGHandle handle, const katana::Uri& current_rdg_dir,
      std::unique_ptr<tsuba::WriteGroup>& write_group);

  /// Extract metadata from an previous storage format topology
  /// Only should be used when transitioning from a previous storage format topology
  /// *ONLY USE THIS FOR BACKWARDS COMPATIBILITY*
  /// bool storage_valid: used to control whether this topology should be written out on store. If storage is valid, no need to write the topology out to a file.
  katana::Result<void> ExtractMetadata(
      const katana::Uri& metadata_dir, uint64_t num_nodes, uint64_t num_edges,
      bool storage_valid = false) {
    KATANA_LOG_WARN(
        "Extracting metadata from csr topology. Store the graph to avoid this "
        "(small) overhead");
    KATANA_LOG_VASSERT(
        num_topologies_ == 1,
        "must have one and only one topology when transition from previous "
        "storage format topology");

    //assume that the one and only topology we have is our previous format topology
    //can't use GetTopology() to find this topology since we don't have any valid metadata to search by
    RDGTopology* topology = &(topology_set_.at(0));

    // only bind the first part of the topology file to save on memory since we only want to extract the metadata
    // this magic number, 4, is just binding enough of the file to extract num_nodes and num_edges
    KATANA_CHECKED_CONTEXT(
        topology->Bind(metadata_dir, 0, 4, true),
        "binding previous format topology file");
    KATANA_CHECKED_CONTEXT(
        topology->MapMetadataExtract(num_nodes, num_edges, storage_valid),
        "mapping previous format topology file");
    KATANA_CHECKED_CONTEXT(
        topology->unbind_file_storage(),
        "unbinding previous format topology file");

    return katana::ResultSuccess();
  }

  katana::Result<void> UnbindAllTopologyFile() {
    for (size_t i = 0; i < num_topologies_; i++) {
      KATANA_CHECKED(topology_set_.at(i).unbind_file_storage());
    }
    return katana::ResultSuccess();
  }

  bool Equals(const RDGTopologyManager& other) const {
    if (num_topologies_ != other.num_topologies_) {
      return false;
    }
    for (size_t i = 0; i < num_topologies_; i++) {
      if (!topology_set_.at(i).Equals(other.topology_set_.at(i))) {
        return false;
      }
    }
    return true;
  }

  /// Create an RDGTopologyManager instance from a set of entries
  static katana::Result<tsuba::RDGTopologyManager> Make(
      PartitionTopologyMetadata* topology_metadata);

private:
  // set of mapped TopologyFiles
  RDGTopologySet topology_set_{};
  size_t num_topologies_{0};
};

}  // namespace tsuba

#endif

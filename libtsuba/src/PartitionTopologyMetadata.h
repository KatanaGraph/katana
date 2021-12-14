#ifndef KATANA_LIBTSUBA_PARTITIONTOPOLOGYMETADATA_H_
#define KATANA_LIBTSUBA_PARTITIONTOPOLOGYMETADATA_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/config.h"
#include "tsuba/Errors.h"
#include "tsuba/RDGTopology.h"
#include "tsuba/tsuba.h"

namespace tsuba {

// Definitions
class KATANA_EXPORT PartitionTopologyMetadataEntry {
public:
  std::string path_{""};
  uint64_t num_edges_{0};
  uint64_t num_nodes_{0};
  bool edge_index_to_property_index_map_present_{false};
  bool node_index_to_property_index_map_present_{false};
  bool edge_condensed_type_id_map_present_{false};
  uint64_t edge_condensed_type_id_map_size_{0};
  bool node_condensed_type_id_map_present_{false};
  uint64_t node_condensed_type_id_map_size_{0};
  tsuba::RDGTopology::TopologyKind topology_state_{-1};
  tsuba::RDGTopology::TransposeKind transpose_state_{-1};
  tsuba::RDGTopology::EdgeSortKind edge_sort_state_{-1};
  tsuba::RDGTopology::NodeSortKind node_sort_state_{-1};

  // control variables

  /// a PartitionTopologyMetadataEntry marked as invalid has been superseded and should not be stored
  bool invalid_{false};

  /// The old location of the topology file, used during relocation of the RDG
  std::string old_path_{""};

  void Update(
      std::string path, uint64_t num_edges, uint64_t num_nodes,
      bool edge_index_to_property_index_map_present,
      bool node_index_to_property_index_map_present,
      uint64_t edge_condensed_type_id_map_size,
      bool edge_condensed_type_id_map_present,
      uint64_t node_condensed_type_id_map_size,
      bool node_condensed_type_id_map_present,
      tsuba::RDGTopology::TopologyKind topology_state,
      tsuba::RDGTopology::TransposeKind transpose_state,
      tsuba::RDGTopology::EdgeSortKind edge_sort_state,
      tsuba::RDGTopology::NodeSortKind node_sort_state) {
    path_ = path;
    Update(
        num_edges, num_nodes, edge_index_to_property_index_map_present,
        node_index_to_property_index_map_present,
        edge_condensed_type_id_map_size, edge_condensed_type_id_map_present,
        node_condensed_type_id_map_size, node_condensed_type_id_map_present,
        topology_state, transpose_state, edge_sort_state, node_sort_state);
  }

  void Update(
      uint64_t num_edges, uint64_t num_nodes,
      bool edge_index_to_property_index_map_present,
      bool node_index_to_property_index_map_present,
      uint64_t edge_condensed_type_id_map_size,
      bool edge_condensed_type_id_map_present,
      uint64_t node_condensed_type_id_map_size,
      bool node_condensed_type_id_map_present,
      tsuba::RDGTopology::TopologyKind topology_state,
      tsuba::RDGTopology::TransposeKind transpose_state,
      tsuba::RDGTopology::EdgeSortKind edge_sort_state,
      tsuba::RDGTopology::NodeSortKind node_sort_state) {
    num_edges_ = num_edges;
    num_nodes_ = num_nodes;
    edge_index_to_property_index_map_present_ =
        edge_index_to_property_index_map_present;
    node_index_to_property_index_map_present_ =
        node_index_to_property_index_map_present;
    edge_condensed_type_id_map_size_ = edge_condensed_type_id_map_size;
    edge_condensed_type_id_map_present_ = edge_condensed_type_id_map_present;
    node_condensed_type_id_map_size_ = node_condensed_type_id_map_size;
    node_condensed_type_id_map_present_ = node_condensed_type_id_map_present;
    topology_state_ = topology_state;
    transpose_state_ = transpose_state;
    edge_sort_state_ = edge_sort_state;
    node_sort_state_ = node_sort_state;
  }

  void set_invalid() { invalid_ = true; }
};

// set of PartitionTopologyMetadataEntry objects
using PartitionTopologyMetadataEntries =
    std::array<PartitionTopologyMetadataEntry, tsuba::kMaxNumTopologies>;

class KATANA_EXPORT PartitionTopologyMetadata {
public:
  PartitionTopologyMetadata() = default;

  PartitionTopologyMetadataEntry* GetEntry(uint32_t index);

  PartitionTopologyMetadataEntry* Append(PartitionTopologyMetadataEntry entry);

  uint32_t num_entries() const { return num_entries_; }
  void set_num_entries(uint32_t num) { num_entries_ = num; }

  const PartitionTopologyMetadataEntries& Entries() const { return entries_; }

  // actual relocation occurs during RDG::Store, blanking the paths indicates we must relocate
  void ChangeStorageLocation() {
    for (size_t i = 0; i < num_entries_; i++) {
      entries_.at(i).old_path_ = entries_.at(i).path_;
      entries_.at(i).path_ = "";
    }
  }

  katana::Result<void> Validate() const;

  friend void to_json(
      nlohmann::json& j, const PartitionTopologyMetadata& topomd);
  friend void from_json(
      const nlohmann::json& j, PartitionTopologyMetadata& topomd);

private:
  PartitionTopologyMetadataEntries entries_ = {};
  uint32_t num_entries_ = 0;

  katana::Result<void> ValidateEntry(
      const PartitionTopologyMetadataEntry& entry) const;

  std::string EntryToString(const PartitionTopologyMetadataEntry& entry) const;
};

static PartitionTopologyMetadataEntry invalid_metadata_entry =
    PartitionTopologyMetadataEntry();

}  // namespace tsuba

#endif

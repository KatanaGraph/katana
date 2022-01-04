#ifndef KATANA_LIBTSUBA_KATANA_PARTITIONMETADATA_H_
#define KATANA_LIBTSUBA_KATANA_PARTITIONMETADATA_H_

#include <cstdint>
#include <utility>

namespace katana {

struct PartitionMetadata {
  uint32_t policy_id_{0};
  bool transposed_{false};
  bool is_outgoing_edge_cut_{false};  // TODO(thunt) deprecated
  bool is_incoming_edge_cut_{false};  // TODO(thunt) deprecated
  uint64_t num_global_nodes_{0UL};
  uint64_t max_global_node_id_{0UL};
  uint64_t num_global_edges_{0UL};
  uint64_t num_edges_{0UL};
  uint32_t num_nodes_{0};
  uint32_t num_owned_{0};
  std::pair<uint32_t, uint32_t> cartesian_grid_{0, 0};
};

}  // namespace katana

#endif

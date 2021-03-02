#ifndef KATANA_LIBTSUBA_TSUBA_PARTITIONMETADATA_H_
#define KATANA_LIBTSUBA_TSUBA_PARTITIONMETADATA_H_

#include <cstdint>
#include <utility>

namespace tsuba {

struct PartitionMetadata {
  uint32_t policy_id_{0};
  bool transposed_{false};
  bool is_outgoing_edge_cut_{false};
  bool is_incoming_edge_cut_{false};
  uint64_t num_global_nodes_{0UL};
  uint64_t num_global_edges_{0UL};
  uint64_t num_edges_{0UL};
  uint32_t num_nodes_{0};
  uint32_t num_owned_{0};
  std::pair<uint32_t, uint32_t> cartesian_grid_{0, 0};
};

}  // namespace tsuba

#endif

#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_BFS_BFS_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_BFS_BFS_H_

#include <iostream>

#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

namespace katana::analytics {

/// A computational plan to for BFS, specifying the algorithm and any parameters
/// associated with it.
class BfsPlan : public Plan {
public:
  enum Algorithm { kAsyncTile = 0, kAsync, kSyncTile, kSync };

private:
  Algorithm algorithm_;
  ptrdiff_t edge_tile_size_;

  BfsPlan(
      Architecture architecture, Algorithm algorithm, ptrdiff_t edge_tile_size)
      : Plan(architecture),
        algorithm_(algorithm),
        edge_tile_size_(edge_tile_size) {}

public:
  BfsPlan() : BfsPlan{kCPU, kSyncTile, 256} {}

  Algorithm algorithm() const { return algorithm_; }
  ptrdiff_t edge_tile_size() const { return edge_tile_size_; }

  static BfsPlan AsyncTile(ptrdiff_t edge_tile_size = 256) {
    return {kCPU, kAsyncTile, edge_tile_size};
  }

  static BfsPlan Async() { return {kCPU, kAsync, 0}; }

  static BfsPlan SyncTile(ptrdiff_t edge_tile_size = 256) {
    return {kCPU, kSyncTile, edge_tile_size};
  }

  static BfsPlan Sync() { return {kCPU, kSync, 0}; }

  static BfsPlan FromAlgorithm(Algorithm algo) {
    switch (algo) {
    case kAsync:
      return Async();
    case kAsyncTile:
      return AsyncTile();
    case kSync:
      return Sync();
    case kSyncTile:
      return SyncTile();
    default:
      return {};
    }
  }
};

/// The tag for the output property of BFS in PropertyGraphs.
using BfsNodeDistance = katana::PODProperty<uint32_t>;

/// Compute BFS level of nodes in the graph pfg starting from start_node. The
/// result is stored in a property named by output_property_name. The plan
/// controls the algorithm and parameters used to compute the BFS.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> Bfs(
    PropertyFileGraph* pfg, size_t start_node,
    const std::string& output_property_name, BfsPlan algo = {});

/// Do a quick validation of the results of a BFS computation where the results
/// are stored in property_name. This function does not do an exhaustive check.
/// The results are approximate and may have false-negatives.
/// @return a failure if the BFS results do not pass validation or if there is a
///     failure during checking.
KATANA_EXPORT Result<void> BfsAssertValid(
    PropertyFileGraph* pfg, const std::string& property_name);

/// Statistics about a graph that can be extracted from the results of BFS.
struct KATANA_EXPORT BfsStatistics {
  /// The source node for the distances.
  uint32_t source_node;
  /// The maximum distance across all nodes.
  uint32_t max_distance;
  /// The sum of all node distances.
  uint64_t total_distance;
  /// The number of nodes reachable from the source node.
  uint32_t n_reached_nodes;

  float average_distance() { return float(total_distance) / n_reached_nodes; }

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout);

  /// Compute the statistics of BFS results stored in property_name.
  static katana::Result<BfsStatistics> Compute(
      katana::PropertyFileGraph* pfg, const std::string& property_name);
};

}  // namespace katana::analytics

#endif

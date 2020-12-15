#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_BFS_BFS_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_BFS_BFS_H_

#include "galois/analytics/Plan.h"
#include "galois/analytics/Utils.h"

namespace galois::analytics {

/// A computational plan to for BFS, specifying the algorithm and any parameters
/// associated with it.
class BfsPlan : Plan {
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

  static BfsPlan Automatic() { return {}; }

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
      return Automatic();
    }
  }
};

/// The tag for the output property of BFS in PropertyGraphs.
using BfsNodeDistance = galois::PODProperty<uint32_t>;
// TODO: Should this be a struct to make it distinct from other types? Or should
//  it be an alias like this so it's compatible with other properties of the
//  same type?

/// Compute BFS level of nodes in the graph pfg starting from start_node. The
/// result is stored in a property named by output_property_name. The plan
/// controls the algorithm and parameters used to compute the BFS.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
GALOIS_EXPORT Result<void> Bfs(
    graphs::PropertyFileGraph* pfg, size_t start_node,
    const std::string& output_property_name,
    BfsPlan algo = BfsPlan::Automatic());

/// Compute BFS level of nodes in the graph pfg starting from start_node. The
/// result is stored in the node data of the graph. The plan controls the
/// algorithm and parameters used to compute the BFS.
GALOIS_EXPORT Result<void> Bfs(
    graphs::PropertyGraph<std::tuple<BfsNodeDistance>, std::tuple<>>& graph,
    size_t start_node, BfsPlan algo = BfsPlan::Automatic());

}  // namespace galois::analytics

#endif

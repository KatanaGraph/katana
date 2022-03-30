#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_BFS_BFS_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_BFS_BFS_H_

#include <iostream>

#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

namespace katana::analytics {

/// A computational plan to for BFS, specifying the algorithm and any parameters
/// associated with it.
class BfsPlan : public Plan {
public:
  enum Algorithm {
    kAsynchronousTile = 0,
    kAsynchronous,
    kSynchronousTile,
    kSynchronous,
    kSynchronousDirectOpt
  };

  static const int kDefaultEdgeTileSize = 256;
  static const uint32_t kDefaultAlpha = 15;
  static const uint32_t kDefaultBeta = 18;

private:
  Algorithm algorithm_;
  ptrdiff_t edge_tile_size_;
  uint32_t alpha_;
  uint32_t beta_;

  BfsPlan(
      Architecture architecture, Algorithm algorithm, ptrdiff_t edge_tile_size,
      uint32_t alpha, uint32_t beta)
      : Plan(architecture),
        algorithm_(algorithm),
        edge_tile_size_(edge_tile_size),
        alpha_(alpha),
        beta_(beta) {}

public:
  BfsPlan()
      : BfsPlan{
            kCPU, kSynchronousDirectOpt, kDefaultEdgeTileSize, kDefaultAlpha,
            kDefaultBeta} {}

  Algorithm algorithm() const { return algorithm_; }
  ptrdiff_t edge_tile_size() const { return edge_tile_size_; }
  uint32_t alpha() const { return alpha_; }
  uint32_t beta() const { return beta_; }

  static BfsPlan AsynchronousTile(
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) {
    return {kCPU, kAsynchronousTile, edge_tile_size, 0, 0};
  }

  static BfsPlan Asynchronous() { return {kCPU, kAsynchronous, 0, 0, 0}; }

  static BfsPlan SynchronousTile(
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) {
    return {kCPU, kSynchronousTile, edge_tile_size, 0, 0};
  }

  static BfsPlan Synchronous() { return {kCPU, kSynchronous, 0, 0, 0}; }

  static BfsPlan SynchronousDirectOpt(
      uint32_t alpha = kDefaultAlpha, uint32_t beta = kDefaultBeta) {
    return {kCPU, kSynchronousDirectOpt, 0, alpha, beta};
  }
};

/// Compute BFS parent of nodes in the graph pg starting from start_node. The
/// result is stored in a property named by output_property_name. The plan
/// controls the algorithm and parameters used to compute the BFS.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> Bfs(
    const std::shared_ptr<PropertyGraph>& pg, uint32_t start_node,
    const std::string& output_property_name, katana::TxnContext* txn_ctx,
    BfsPlan algo = {});

/// Do a quick validation of the results of a BFS computation where the results
/// are stored in property_name. This function does do an exhaustive check.
/// @return a failure if the BFS results do not pass validation or if there is a
///     failure during checking.
KATANA_EXPORT Result<void> BfsAssertValid(
    const std::shared_ptr<PropertyGraph>& pg, uint32_t source,
    const std::string& property_name);

/// Statistics about a graph that can be extracted from the results of BFS.
struct KATANA_EXPORT BfsStatistics {
  /// The number of nodes reachable from the source node.
  uint64_t n_reached_nodes;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  /// Compute the statistics of BFS results stored in property_name.
  static katana::Result<BfsStatistics> Compute(
      const std::shared_ptr<PropertyGraph>& pg,
      const std::string& property_name);
};

}  // namespace katana::analytics

#endif

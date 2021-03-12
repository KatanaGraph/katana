#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_CONNECTEDCOMPONENTS_CONNECTEDCOMPONENTS_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_CONNECTEDCOMPONENTS_CONNECTEDCOMPONENTS_H_

#include <iostream>

#include "katana/AtomicHelpers.h"
#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

// API

namespace katana::analytics {

/// A computational plan to for ConnectedComponents, specifying the algorithm and any
/// parameters associated with it.
class ConnectedComponentsPlan : public Plan {
public:
  /// Algorithm selectors for Connected-components
  enum Algorithm {
    kSerial,
    kLabelProp,
    kSynchronous,
    kAsynchronous,
    kEdgeAsynchronous,
    kEdgeTiledAsynchronous,
    kBlockedAsynchronous,
    kAfforest,
    kEdgeAfforest,
    kEdgeTiledAfforest
  };

  static const ptrdiff_t kDefaultEdgeTileSize = 512;
  static const uint32_t kDefaultNeighborSampleSize = 2;
  static const uint32_t kDefaultComponentSampleFrequency = 1024;

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;
  ptrdiff_t edge_tile_size_;
  uint32_t neighbor_sample_size_;
  uint32_t component_sample_frequency_;

  ConnectedComponentsPlan(
      Architecture architecture, Algorithm algorithm, ptrdiff_t edge_tile_size,
      uint32_t neighbor_sample, uint32_t component_sample_frequency)
      : Plan(architecture),
        algorithm_(algorithm),
        edge_tile_size_(edge_tile_size),
        neighbor_sample_size_(neighbor_sample),
        component_sample_frequency_(component_sample_frequency) {}

public:
  // kChunkSize is a fixed const int (default value: 1)
  static const int kChunkSize;

  ConnectedComponentsPlan()
      : ConnectedComponentsPlan{
            kCPU, kAfforest, 0, kDefaultNeighborSampleSize,
            kDefaultComponentSampleFrequency} {}

  Algorithm algorithm() const { return algorithm_; }
  ptrdiff_t edge_tile_size() const { return edge_tile_size_; }
  uint32_t neighbor_sample_size() const { return neighbor_sample_size_; }
  uint32_t component_sample_frequency() const {
    return component_sample_frequency_;
  }

  /// Serial connected components algorithm. Uses the union-find datastructure.
  static ConnectedComponentsPlan Serial() { return {kCPU, kSerial, 0, 0, 0}; }

  /// Label propagation push-style algorithm. Initially, all nodes are in
  /// their own component IDs (same as their node IDs). Then, the component
  /// IDs are set to the minimum component ID in their neighborhood.
  static ConnectedComponentsPlan LabelProp() {
    return {kCPU, kLabelProp, 0, 0, 0};
  }

  /// Synchronous connected components algorithm.  Initially all nodes are in
  /// their own component. Then, we merge endpoints of edges to form the spanning
  /// tree. Merging is done in two phases to simplify concurrent updates: (1)
  /// find components and (2) union components.  Since the merge phase does not
  /// do any finds, we only process a fraction of edges at a time; otherwise,
  /// the union phase may unnecessarily merge two endpoints in the same
  /// component.
  static ConnectedComponentsPlan Synchronous() {
    return {kCPU, kSynchronous, 0, 0, 0};
  }

  /// Unlike Synchronous algorithm, Asynchronous doesn't restrict path compression
  /// (UnionFind data structure) and can perform unions and finds concurrently.
  static ConnectedComponentsPlan Asynchronous() {
    return {kCPU, kAsynchronous, 0, 0, 0};
  }

  /// Similar to Asynchronous, except that work-item is edge instead of node.
  static ConnectedComponentsPlan EdgeAsynchronous() {
    return {kCPU, kEdgeAsynchronous, 0, 0, 0};
  }

  /// Similar EdgeSynchronous with the work-item as block of edges.
  static ConnectedComponentsPlan EdgeTiledAsynchronous(
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) {
    return {kCPU, kEdgeTiledAsynchronous, edge_tile_size, 0, 0};
  }

  /// Similar Asynchronous with the work-item as block of nodes.
  /// Improves performance of Asynchronous algorithm by following machine topology.
  static ConnectedComponentsPlan BlockedAsynchronous() {
    return {kCPU, kBlockedAsynchronous, 0, 0, 0};
  }

  /// Connected-components using Afforest sampling.
  /// [1] M. Sutton, T. Ben-Nun and A. Barak, "Optimizing Parallel Graph
  /// Connectivity Computation via Subgraph Sampling," 2018 IEEE International
  /// Parallel and Distributed Processing Symposium (IPDPS), Vancouver, BC, 2018,
  /// pp. 12-21.
  static ConnectedComponentsPlan Afforest(
      uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
      uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) {
    return {
        kCPU, kAfforest, 0, neighbor_sample_size, component_sample_frequency};
  }

  /// Connected-components using Afforest sampling with edge as work-item.
  /// [1] M. Sutton, T. Ben-Nun and A. Barak, "Optimizing Parallel Graph
  /// Connectivity Computation via Subgraph Sampling," 2018 IEEE International
  /// Parallel and Distributed Processing Symposium (IPDPS), Vancouver, BC, 2018,
  /// pp. 12-21.
  static ConnectedComponentsPlan EdgeAfforest(
      uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
      uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) {
    return {
        kCPU, kEdgeAfforest, 0, neighbor_sample_size,
        component_sample_frequency};
  }

  /// Connected-components using Afforest sampling with block of edges as work-item.
  /// [1] M. Sutton, T. Ben-Nun and A. Barak, "Optimizing Parallel Graph
  /// Connectivity Computation via Subgraph Sampling," 2018 IEEE International
  /// Parallel and Distributed Processing Symposium (IPDPS), Vancouver, BC, 2018,
  /// pp. 12-21.
  static ConnectedComponentsPlan EdgeTiledAfforest(
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize,
      uint32_t neighbor_sample_size = kDefaultNeighborSampleSize,
      uint32_t component_sample_frequency = kDefaultComponentSampleFrequency) {
    return {
        kCPU, kEdgeAfforest, edge_tile_size, neighbor_sample_size,
        component_sample_frequency};
  }
};

/// Compute the Connected-components for pg. The pg is expected to be
/// symmetric.
/// The algorithm, neighbor sample size and component sample frequency and tile size
/// parameters can be specified, but have reasonable defaults. Not all parameters
/// are used by the algorithms.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> ConnectedComponents(
    PropertyGraph* pg, const std::string& output_property_name,
    ConnectedComponentsPlan plan = ConnectedComponentsPlan());

KATANA_EXPORT Result<void> ConnectedComponentsAssertValid(
    PropertyGraph* pg, const std::string& property_name);

struct KATANA_EXPORT ConnectedComponentsStatistics {
  /// Total number of unique components in the graph.
  uint64_t total_components;
  /// Total number of components with more than 1 node.
  uint64_t total_non_trivial_components;
  /// The number of nodes present in the largest component.
  uint64_t largest_component_size;
  /// The ratio of nodes present in the largest component.
  double largest_component_ratio;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  static katana::Result<ConnectedComponentsStatistics> Compute(
      katana::PropertyGraph* pg, const std::string& property_name);
};

}  // namespace katana::analytics
#endif

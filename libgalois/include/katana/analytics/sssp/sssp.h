#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_SSSP_SSSP_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_SSSP_SSSP_H_

#include "katana/AtomicHelpers.h"
#include "katana/analytics/BfsSsspImplementationBase.h"
#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

namespace katana::analytics {

/// A computational plan to for SSSP, specifying the algorithm and any
/// parameters associated with it.
class SsspPlan : public Plan {
public:
  /// Algorithm selectors for Single-Source Shortest Path
  enum Algorithm {
    kDeltaTile,
    kDeltaStep,
    kDeltaStepBarrier,
    // TODO(gill): Do we want to expose serial implementations at all?
    kSerialDeltaTile,
    kSerialDelta,
    kDijkstraTile,
    kDijkstra,
    kTopological,
    kTopologicalTile,
    kAutomatic,
  };

  static const int kDefaultDelta = 13;
  static const int kDefaultEdgeTileSize = 512;

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;
  unsigned delta_;
  ptrdiff_t edge_tile_size_;
  // TODO: should chunk_size be in the plan? Or fixed?
  //  It cannot be in the plan currently because it is a template parameter and
  //  cannot be easily changed since the value is statically passed on to
  //  FixedSizeRing.
  //unsigned chunk_size_ = 64;

  SsspPlan(
      Architecture architecture, Algorithm algorithm, unsigned delta,
      ptrdiff_t edge_tile_size)
      : Plan(architecture),
        algorithm_(algorithm),
        delta_(delta),
        edge_tile_size_(edge_tile_size) {}

public:
  SsspPlan() : SsspPlan{kCPU, kAutomatic, 0, 0} {}

  SsspPlan(const katana::PropertyFileGraph* pfg) : Plan(kCPU) {
    bool isPowerLaw = IsApproximateDegreeDistributionPowerLaw(*pfg);
    if (isPowerLaw) {
      *this = DeltaStep();
    } else {
      *this = DeltaStepBarrier();
    }
  }

  Algorithm algorithm() const { return algorithm_; }
  unsigned delta() const { return delta_; }
  ptrdiff_t edge_tile_size() const { return edge_tile_size_; }

  static SsspPlan DeltaTile(
      unsigned delta = kDefaultDelta,
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) {
    return {kCPU, kDeltaTile, delta, edge_tile_size};
  }

  static SsspPlan DeltaStep(unsigned delta = kDefaultDelta) {
    return {kCPU, kDeltaStep, delta, 0};
  }

  static SsspPlan DeltaStepBarrier(unsigned delta = kDefaultDelta) {
    return {kCPU, kDeltaStepBarrier, delta, 0};
  }

  static SsspPlan SerialDeltaTile(
      unsigned delta = kDefaultDelta,
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) {
    return {kCPU, kSerialDeltaTile, delta, edge_tile_size};
  }

  static SsspPlan SerialDelta(unsigned delta = kDefaultDelta) {
    return {kCPU, kSerialDelta, delta, 0};
  }

  static SsspPlan DijkstraTile(
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) {
    return {kCPU, kDijkstraTile, 0, edge_tile_size};
  }

  static SsspPlan Dijkstra() { return {kCPU, kDijkstra, 0, 0}; }

  static SsspPlan Topological() { return {kCPU, kTopological, 0, 0}; }

  static SsspPlan TopologicalTile(
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) {
    return {kCPU, kTopologicalTile, 0, edge_tile_size};
  }
};

/// Compute the Single-Source Shortest Path for pfg starting from start_node.
/// The edge weights are taken from the property named
/// edge_weight_property_name (which may be a 32- or 64-bit sign or unsigned
/// int), and the computed path lengths are stored in the property named
/// output_property_name (as uint32_t). The algorithm and delta stepping
/// parameter can be specified, but have reasonable defaults.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> Sssp(
    PropertyFileGraph* pfg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name, SsspPlan plan = {});

KATANA_EXPORT Result<void> SsspAssertValid(
    PropertyFileGraph* pfg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name);

struct KATANA_EXPORT SsspStatistics {
  /// The maximum distance across all nodes.
  double max_distance;
  /// The sum of all node distances.
  double total_distance;
  /// The number of nodes reachable from the source node.
  uint32_t n_reached_nodes;

  double average_distance() const { return total_distance / n_reached_nodes; }

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  static katana::Result<SsspStatistics> Compute(
      PropertyFileGraph* pfg, const std::string& output_property_name);
};

}  // namespace katana::analytics

#endif

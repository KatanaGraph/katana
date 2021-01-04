#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_SSSP_SSSP_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_SSSP_SSSP_H_

#include <galois/analytics/Plan.h>

#include "galois/AtomicHelpers.h"
#include "galois/analytics/BfsSsspImplementationBase.h"
#include "galois/analytics/Utils.h"

// API

namespace galois::analytics {

/// A computational plan to for SSSP, specifying the algorithm and any
/// parameters associated with it.
class SsspPlan : Plan {
public:
  /// Algorithm selectors for Single-Source Shortest Path
  enum Algorithm {
    kDeltaTile,
    kDeltaStep,
    kDeltaStepBarrier,
    kSerialDeltaTile,  // TODO: Do we want to expose these at all?
    kSerialDelta,
    kDijkstraTile,
    kDijkstra,
    kTopo,
    kTopoTile,
    kAutomatic,
  };

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

  Algorithm algorithm() const { return algorithm_; }
  unsigned delta() const { return delta_; }
  ptrdiff_t edge_tile_size() const { return edge_tile_size_; }

  static SsspPlan DeltaTile(
      unsigned delta = 13, ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kDeltaTile, delta, edge_tile_size};
  }

  static SsspPlan DeltaStep(unsigned delta = 13) {
    return {kCPU, kDeltaStep, delta, 0};
  }

  static SsspPlan DeltaStepBarrier(unsigned delta = 13) {
    return {kCPU, kDeltaStepBarrier, delta, 0};
  }

  static SsspPlan SerialDeltaTile(
      unsigned delta = 13, ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kSerialDeltaTile, delta, edge_tile_size};
  }

  static SsspPlan SerialDelta(unsigned delta = 13) {
    return {kCPU, kSerialDelta, delta, 0};
  }

  static SsspPlan DijkstraTile(ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kDijkstraTile, 0, edge_tile_size};
  }

  static SsspPlan Dijkstra() { return {kCPU, kDijkstra, 0, 0}; }

  // TODO: Should this be "Topological"
  static SsspPlan Topo() { return {kCPU, kTopo, 0, 0}; }

  static SsspPlan TopoTile(ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kTopoTile, 0, edge_tile_size};
  }

  static SsspPlan Automatic() { return {}; }

  static SsspPlan Automatic(const galois::graphs::PropertyFileGraph* pfg) {
    // TODO: What to do about const cast? We know we don't modify pfg, but there
    //  is no way to construct a const PropertyGraph.
    auto graph =
        galois::graphs::PropertyGraph<std::tuple<>, std::tuple<>>::Make(
            const_cast<galois::graphs::PropertyFileGraph*>(pfg), {}, {});
    if (!graph)
      GALOIS_LOG_FATAL(
          "PropertyGraph should always be constructable here: {}",
          graph.error());
    galois::StatTimer autoAlgoTimer("SSSP_Automatic_Algorithm_Selection");
    autoAlgoTimer.start();
    bool isPowerLaw = isApproximateDegreeDistributionPowerLaw(graph.value());
    autoAlgoTimer.stop();
    if (isPowerLaw) {
      return DeltaStep();
    } else {
      return DeltaStepBarrier();
    }
  }
};

template <typename Weight>
struct SsspNodeDistance {
  using ArrowType = typename arrow::CTypeTraits<Weight>::ArrowType;
  using ViewType = galois::PODPropertyView<std::atomic<Weight>>;
};

template <typename Weight>
using SsspEdgeWeight = galois::PODProperty<Weight>;

/// Compute the Single-Source Shortest Path for pfg starting from start_node.
/// The edge weights are taken from the property named
/// edge_weight_property_name (which may be a 32- or 64-bit sign or unsigned
/// int), and the computed path lengths are stored in the property named
/// output_property_name (as uint32_t). The algorithm and delta stepping
/// parameter can be specified, but have reasonable defaults.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
GALOIS_EXPORT Result<void> Sssp(
    graphs::PropertyFileGraph* pfg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name,
    SsspPlan plan = SsspPlan::Automatic());

GALOIS_EXPORT Result<bool> SsspValidate(
    graphs::PropertyFileGraph* pfg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name);

struct GALOIS_EXPORT SsspStatistics {
  /// The maximum distance across all nodes.
  double max_distance;
  /// The sum of all node distances.
  double total_distance;
  /// The number of nodes reachable from the source node.
  uint32_t n_reached_nodes;

  double average_distance() { return total_distance / n_reached_nodes; }

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout);

  static galois::Result<SsspStatistics> Compute(
      graphs::PropertyFileGraph* pfg, const std::string& output_property_name);
};

}  // namespace galois::analytics

#endif

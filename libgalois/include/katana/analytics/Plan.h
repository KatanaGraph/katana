#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_PLAN_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_PLAN_H_

namespace katana::analytics {

enum Architecture {
  /// Local execution using CPUs only
  kCPU,
  /// Local execution using mostly GPUs
  kGPU,
  /// Distributed execution using CPUs
  kDistributedCPU,
  /// Distributed execution using GPUs
  kDistributedGPU
};

/// The base class for abstract algorithm execution plans.
///
/// Execution plans contain any tuning parameters the abstract algorithm requires. In general, this will include
/// selecting the concrete algorithm to use and providing its tuning parameters (e.g., tile size). Plans do not affect
/// the result (beyond potentially returning a different valid result in the case of non-deterministic algorithms or
/// floating-point operations).
///
/// All plans provide static methods to create a plan each concrete algorithm and constructors for any automatic
/// heuristics. The automatic constructors may either take no input and make decisions at run time, use fixed default
/// parameters, or take a graph to be analyzed to determine appropriate parameters. A plan created automatically for one
/// graph can still be used with other graphs, though the algorithm will be tuned for the original graph and may not be
/// efficient. This may be useful, for instance, to use a sampled subgraph to compute a plan for use on a the original
/// larger graph.
class Plan {
protected:
  Architecture architecture_;

  Plan(Architecture architecture) : architecture_(architecture) {}

public:
  /// The architecture on which the algorithm will run.
  Architecture architecture() const { return architecture_; }
};

}  // namespace katana::analytics

#endif  //KATANA_PLAN_H_

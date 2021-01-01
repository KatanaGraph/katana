#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_PLAN_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_PLAN_H_

namespace galois::analytics {

enum Architecture {
  /// Local execution using CPUs only
  kCPU,
  /// Local execution using mostly GPUs
  kGPU,
  /// Distributed execution using both CPUs and GPUs
  kDistributed
};

class Plan {
protected:
  Architecture architecture_;

  Plan(Architecture architecture) : architecture_(architecture) {}

public:
  Architecture architecture() const { return architecture_; }
};

}  // namespace galois::analytics

#endif  //GALOIS_PLAN_H_

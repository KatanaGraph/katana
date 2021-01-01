#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_PLAN_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_PLAN_H_

namespace katana::analytics {

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

}  // namespace katana::analytics

#endif  //KATANA_PLAN_H_

#pragma once

#include <memory>
#include <unordered_map>

#include "katana/Manager.h"
#include "katana/Result.h"
#include "katana/config.h"

namespace katana {
/// Define a memory policy

/// The policy virtual base class
class KATANA_EXPORT MemoryPolicy {
public:
  virtual ~MemoryPolicy();

  /// Given the current memory counts and whatever OS sources the policy consults,
  /// how much standby memory should we reclaim right now?
  virtual count_t ReclaimForMemoryPressure(
      [[maybe_unused]] count_t active,
      [[maybe_unused]] count_t standby) const = 0;

  /// Given the current memory counts and whatever OS sources the policy consults,
  /// should we refuse discretionary allocations?
  virtual bool MemoryPressureHigh(
      [[maybe_unused]] count_t active,
      [[maybe_unused]] count_t standby) const = 0;

  /// Given the current memory counts and whatever OS sources the policy consults,
  /// should we clean up and exit?
  virtual bool KillSelfForLackOfMemory(
      [[maybe_unused]] count_t active,
      [[maybe_unused]] count_t standby) const = 0;

  /// Utility function to find out our OOM score from Linux
  static uint64_t OOMScore();

  struct MemInfo;
  struct Thresholds {
    double high_used_ratio_threshold;
    double kill_used_ratio_threshold;
  };

protected:
  MemoryPolicy(Thresholds thresholds);
  void UpdateMemInfo(MemInfo* mem_info, count_t active, count_t standby) const;

  count_t physical() const { return physical_; }
  double high_used_ratio_threshold() const {
    return thresholds_.high_used_ratio_threshold;
  }
  double kill_used_ratio_threshold() const {
    return thresholds_.kill_used_ratio_threshold;
  }

private:
  count_t physical_;
  Thresholds thresholds_;
};

/// Memory policy that just tries to avoid the OOM killer and does little to evict
/// unused data from memory
class KATANA_EXPORT MemoryPolicyMinimal : public MemoryPolicy {
public:
  MemoryPolicyMinimal();
  count_t ReclaimForMemoryPressure(
      count_t active, count_t standby) const override;
  bool MemoryPressureHigh(count_t active, count_t standby) const override;
  bool KillSelfForLackOfMemory(count_t active, count_t standby) const override;
};

/// Memory policy that prioritizes performance, i.e., it uses memory aggressively
class KATANA_EXPORT MemoryPolicyPerformance : public MemoryPolicy {
public:
  MemoryPolicyPerformance();
  count_t ReclaimForMemoryPressure(
      count_t active, count_t standby) const override;
  bool MemoryPressureHigh(count_t active, count_t standby) const override;
  bool KillSelfForLackOfMemory(count_t active, count_t standby) const override;
};

}  // namespace katana

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
  virtual count_t ReclaimForMemoryPressure(count_t standby) const = 0;

  /// Given the current memory counts and whatever OS sources the policy consults,
  /// should we refuse discretionary allocations?
  virtual bool IsMemoryPressureHigh(count_t standby) const = 0;

  /// Given the current memory counts and whatever OS sources the policy consults,
  /// should we clean up and exit?
  virtual bool KillSelfForLackOfMemory(count_t standby) const = 0;

  void LogMemoryStats(const std::string& message, count_t standby);

  /// Utility function to find out our OOM score from Linux
  static uint64_t OOMScore();
  /// Utility function to find out available memory in the machine
  static uint64_t AvailableMemoryBytes();

  struct MemInfo;
  struct Thresholds {
    double high_used_ratio_threshold;
    double kill_used_ratio_threshold;
    count_t kill_self_oom_threshold;
    count_t high_pressure_oom_threshold;
  };

protected:
  MemoryPolicy(Thresholds thresholds);
  void UpdateMemInfo(MemInfo* mem_info, count_t standby) const;

  count_t physical() const { return physical_; }
  double high_used_ratio_threshold() const {
    return thresholds_.high_used_ratio_threshold;
  }
  double kill_used_ratio_threshold() const {
    return thresholds_.kill_used_ratio_threshold;
  }
  count_t kill_self_oom_threshold() const {
    return thresholds_.kill_self_oom_threshold;
  }
  count_t high_pressure_oom_threshold() const {
    return thresholds_.high_pressure_oom_threshold;
  }

private:
  count_t physical_;
  Thresholds thresholds_;
};

/// Memory policy that just tries to avoid the OOM killer.  Unfortunately, it is aggressive about
/// dumping memory when the OOM score is high, which can be an over reaction.
class KATANA_EXPORT MemoryPolicyMinimal : public MemoryPolicy {
public:
  MemoryPolicyMinimal();
  count_t ReclaimForMemoryPressure(count_t standby) const override;
  bool IsMemoryPressureHigh(count_t standby) const override;
  bool KillSelfForLackOfMemory(count_t standby) const override;
};

/// Memory policy that prioritizes performance, i.e., it uses memory aggressively
class KATANA_EXPORT MemoryPolicyPerformance : public MemoryPolicy {
public:
  MemoryPolicyPerformance();
  count_t ReclaimForMemoryPressure(count_t standby) const override;
  bool IsMemoryPressureHigh(count_t standby) const override;
  bool KillSelfForLackOfMemory(count_t standby) const override;
};

/// Minimize use of memory, but take free memory when it is available.
class KATANA_EXPORT MemoryPolicyMeek : public MemoryPolicy {
public:
  MemoryPolicyMeek();
  count_t ReclaimForMemoryPressure(count_t standby) const override;
  bool IsMemoryPressureHigh(count_t standby) const override;
  bool KillSelfForLackOfMemory(count_t standby) const override;
};

/// Do nothing to ever shed memory.  This will OOM if we occupy too much memory.
class KATANA_EXPORT MemoryPolicyNull : public MemoryPolicy {
public:
  MemoryPolicyNull();
  count_t ReclaimForMemoryPressure(count_t standby) const override;
  bool IsMemoryPressureHigh(count_t standby) const override;
  bool KillSelfForLackOfMemory(count_t standby) const override;
};

}  // namespace katana

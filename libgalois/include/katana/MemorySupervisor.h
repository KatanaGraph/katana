#pragma once

#include <memory>
#include <unordered_map>

#include "katana/Cache.h"
#include "katana/Manager.h"
#include "katana/MemoryPolicy.h"
#include "katana/ProgressTracer.h"
#include "katana/Result.h"
#include "katana/config.h"

namespace katana {
/// The memory supervisor singleton (MS).  Not thread safe.
///
/// The MS controls policy and does bookkeeping.  All memory allocation
/// is done by the system, mostly the C++ standard library.
///
/// The MS interacts with Managers of individual resources, e.g., properties.
/// Managers are generally greedy, and the supervisor coordinates among them.  The
/// managers do not actually allocate memory, they are also bookkeepers.
///
/// The MS does not manage per-allocation tokens, it only manages sizes
/// Clients are trusted to call the proper functions or the MS will make
/// bad decisions.
///
/// The MS does not track active memory.  The problem is that data structures (e.g.,
/// properties) change in size while they are active.  If client code loads a 100 byte
/// property and adds a value, it can store back a 102 byte property.  The MS sees 100
/// bytes made active and 102 bytes made inactive, which makes the count of active
/// bytes negative.

class PropertyManager;

class KATANA_EXPORT MemorySupervisor {
public:
  MemorySupervisor(const MemorySupervisor&) = delete;
  MemorySupervisor(MemorySupervisor&&) = delete;
  MemorySupervisor& operator=(const MemorySupervisor&) = delete;
  MemorySupervisor& operator=(MemorySupervisor&&) = delete;

  // https://codereview.stackexchange.com/questions/173929/modern-c-singleton-template
  static auto& Get() {
    static MemorySupervisor mm_;
    return mm_;
  }

  /// Request permission to allocate \p bytes for standby memory
  /// Returns the number of bytes granted, possibly 0
  count_t GetStandby(const std::string& name, count_t goal);

  /// Notify MS that manager freed \p bytes of standby memory.
  void PutStandby(const std::string& name, count_t bytes);

  /// Manager \p name wants to transition \p bytes from active to standby
  void ActiveToStandby(const std::string& name, count_t bytes);
  /// Manager \p name transitions \p bytes from standby to active.
  /// Managers are always allowed to transition from standby to active
  void StandbyToActive(const std::string& name, count_t bytes);

  /// Give the memory supervisor a chance to release memory.  This is useful to call
  /// If you will be calling a series of allocations for active memory, you can use
  /// this to make sure we aren't holding on to too much standby memory.
  void CheckPressure();

  /// The MemoryPolicy controls decisions about memory allocation, like how
  /// aggressively to deallocate.
  void SetPolicy(std::unique_ptr<MemoryPolicy> policy);

  /// Provide access to a property manager, which manages the property cache
  PropertyManager* GetPropertyManager();
  CacheStats GetPropertyCacheStats() const;
  void LogMemoryStats(const std::string& message);

  /// Calls sysconf
  static uint64_t GetTotalSystemMemory();

private:
  MemorySupervisor();
  /// Make sure our state is sane, log if not
  void SanityCheck();

  /// Get managers to free \p goal bytes of standby memory
  void ReclaimMemory(count_t goal);

  struct ManagerInfo {
    std::unique_ptr<Manager> manager_;
    count_t standby{};
  };
  std::unordered_map<std::string, ManagerInfo> managers_;

  count_t Available() {
    auto rss = static_cast<katana::count_t>(
        katana::ProgressTracer::ParseProcSelfRssBytes());
    if (rss >= physical_) {
      return 0;
    }
    return physical_ - rss;
  }

  std::unique_ptr<MemoryPolicy> policy_;

  /// Sum of all standby memory across all managers
  count_t standby_{};
  void StandbyMinus(ManagerInfo& info, count_t bytes);
  void StandbyPlus(ManagerInfo& info, count_t bytes);

  /// The maximum amount of physical memory the MS plans to use, which should be less
  /// than or equal to the total physical memory in the machine.  There are users of
  /// memory outside our control, like the operating system.
  count_t physical_{};

  /// Statistics: bytes reclaimed
  count_t bytes_reclaimed_{};
};

}  // namespace katana

#pragma once

#include <unordered_map>
#include <memory>

#include "katana/Manager.h"
#include "katana/Result.h"
#include "katana/config.h"

namespace katana {
// The memory manager singleton (MM).  Not thread safe.
// The MM controls policy and does bookkeeping.  All memory allocation
// is done by the system, mostly the C++ standard library.
// The MM does not manage per-allocation tokens, it only manages sizes
// Clients must call the proper functions or the MM will make 
// bad decisions.

class KATANA_EXPORT MemoryManager {
public:
  MemoryManager(const MemoryManager&) = delete;
  MemoryManager(MemoryManager&&) = delete;
  MemoryManager& operator=(const MemoryManager&) = delete;
  MemoryManager& operator=(MemoryManager&&) = delete;

  // https://codereview.stackexchange.com/questions/173929/modern-c-singleton-template
  static auto& MM(){
    static MemoryManager mm_;
    return mm_;
  }

   // Let the MM know about this manager.
  void Register(Manager* manager);

  // This manager is defunct.  
  // \p manager must have zero active and standby memory.
  void Unregister(Manager* manager);

  // Request \p bytes from the MM for active memory
  // Application cannot continue if it does not get memory
  void BorrowActive(Manager* manager, count_t bytes);
  // Request \p bytes from the MM for standby memory
  // Returns the number of bytes granted, possibly 0
  count_t BorrowStandby(Manager* manager, count_t goal);

  // Give active \p bytes back to the MM.
  void ReturnActive(Manager* manager, count_t bytes);
  // Give standby \p bytes back to the MM.
  void ReturnStandby(Manager* manager, count_t bytes);

  /// Manager \p manager wants to transition \p bytes from active to standby
  /// Returns the number of standby bytes, possibly 0.
  count_t ActiveToStandby(Manager* manager, count_t bytes);
  /// Manager \p manager transitions \p bytes from standby to active.
  /// Managers are always allowed to transition from standby to active
 void StandbyToActive(Manager* manager, count_t bytes);

private:
  MemoryManager();
  // Make sure our state is sane, log if not
  void Sanity();
  void LogState(const std::string& str);

  // Get managers to return goal bytes of standby memory
  void ReclaimMemory(count_t goal);
  bool CheckRegistered(Manager* manager);

  struct ManagerInfo {
    ManagerInfo() {
    }
    count_t active{};
    count_t standby{};    
  };
  std::unordered_map<Manager*, ManagerInfo> managers_;

  count_t active_{};
  void ActiveMinus(ManagerInfo& info, count_t bytes);
  void ActivePlus(ManagerInfo& info, count_t bytes);
  count_t standby_{};
  void StandbyMinus(ManagerInfo& info, count_t bytes);
  void StandbyPlus(ManagerInfo& info, count_t bytes);
  count_t Used() {
    return active_ + standby_;
  }
  count_t physical_{};
  count_t Available() {
    return physical_ - Used();
  }
  bool MemoryOversubscribed() {
    return Used() >= physical_;
  }
};

} // namespace katana

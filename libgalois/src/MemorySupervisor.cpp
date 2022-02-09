#include "katana/MemorySupervisor.h"

#include <fstream>

#include "katana/MemoryPolicy.h"
#include "katana/ProgressTracer.h"
#include "katana/Time.h"

using katana::count_t;

namespace {

const std::string sanity_str = "memory manager sanity";
const std::string oversubscribed_str = "memory manager oversubscribed";
const std::string unregister_str = "memory manager unregister";

void
LogState(const std::string& str, count_t active, count_t standby) {
  katana::GetTracer().GetActiveSpan().Log(
      str, {
               {"active", active},
               {"standby", standby},
           });
}

void
KillCheck(katana::MemoryPolicy* policy, count_t active, count_t standby) {
  if (policy->KillSelfForLackOfMemory(active, standby)) {
    LogState(oversubscribed_str, active, standby);
    KATANA_LOG_FATAL("out of memory");
  }
}

}  // anonymous namespace

void
katana::MemorySupervisor::SanityCheck() {
  count_t manager_active{};
  count_t manager_standby{};
  bool logit = false;
  for (auto& [manager, info] : managers_) {
    manager_active += info.active;
    manager_standby += info.standby;
  }
  if (manager_active != active_ || manager_standby != standby_) {
    logit = true;
    KATANA_LOG_WARN(
        "manager active {} manager standby {}", manager_active,
        manager_standby);
  }
  if (active_ < 0) {
    KATANA_LOG_ASSERT(false);
    logit = true;
  }
  if (standby_ < 0) {
    KATANA_LOG_ASSERT(false);
    logit = true;
  }
  if (logit) {
    LogState(sanity_str, active_, standby_);
  }
}

katana::MemorySupervisor::MemorySupervisor() {
  physical_ = GetTotalSystemMemory();
  policy_ = std::make_unique<katana::MemoryPolicyPerformance>(
      MemoryPolicyPerformance());
  auto& tracer = katana::GetTracer();
  tracer.GetActiveSpan().Log(
      "memory manager",
      {{"physical", physical_},
       {"physical_human", katana::BytesToStr("{:.2f}{}", physical_)}});
}

void
katana::MemorySupervisor::StandbyMinus(ManagerInfo& info, count_t bytes) {
  info.standby -= bytes;
  standby_ -= bytes;
}
void
katana::MemorySupervisor::StandbyPlus(ManagerInfo& info, count_t bytes) {
  info.standby += bytes;
  standby_ += bytes;
}
void
katana::MemorySupervisor::ActiveMinus(ManagerInfo& info, count_t bytes) {
  info.active -= bytes;
  active_ -= bytes;
}
void
katana::MemorySupervisor::ActivePlus(ManagerInfo& info, count_t bytes) {
  info.active += bytes;
  active_ += bytes;
}

bool
katana::MemorySupervisor::CheckRegistered(Manager* manager) {
  auto it = managers_.find(manager);
  if (it == managers_.end()) {
    KATANA_LOG_WARN(
        "manager {} is not registered", std::quoted(manager->MemoryCategory()));
    return false;
  }
  return true;
}

void
katana::MemorySupervisor::Register(Manager* manager) {
  managers_[manager] = ManagerInfo();
}

void
katana::MemorySupervisor::Unregister(Manager* manager) {
  if (!CheckRegistered(manager)) {
    return;
  }
  auto& info = managers_.at(manager);
  if (info.active > 0 || info.standby > 0) {
    KATANA_LOG_WARN(
        "Unregister for manager {} with active {} standby {}\n",
        std::quoted(manager->MemoryCategory()), info.active, info.standby);
    LogState(unregister_str, active_, standby_);
  }
  ActiveMinus(info, info.active);
  StandbyMinus(info, info.standby);
  managers_.erase(manager);
}

void
katana::MemorySupervisor::ReclaimMemory(count_t goal) {
  if (goal <= 0) {
    return;
  }
  count_t reclaimed = 0;
  // TODO(witchel) policies should include reclaim in proportion to current use
  for (auto& [manager, info] : managers_) {
    // Manager implementation of FreeStandbyMemory calls MemorySupervisor::ReturnStandby
    auto got = manager->FreeStandbyMemory(goal - reclaimed);
    reclaimed += got;
    if (reclaimed >= goal) {
      break;
    }
  }
}

void
katana::MemorySupervisor::BorrowActive(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return;
  }
  auto& info = managers_.at(manager);
  ActivePlus(info, bytes);
  count_t try_reclaim = policy_->ReclaimForMemoryPressure(active_, standby_);
  ReclaimMemory(try_reclaim);

  SanityCheck();
  KillCheck(policy_.get(), active_, standby_);
}

count_t
katana::MemorySupervisor::BorrowStandby(Manager* manager, count_t goal) {
  if (!CheckRegistered(manager)) {
    return 0;
  }
  count_t try_reclaim = policy_->ReclaimForMemoryPressure(active_, standby_);
  ReclaimMemory(try_reclaim);

  if (policy_->MemoryPressureHigh(active_, standby_)) {
    return 0;
  }
  auto& info = managers_.at(manager);
  StandbyPlus(info, goal);

  KillCheck(policy_.get(), active_, standby_);
  return std::min(goal, Available());
}

void
katana::MemorySupervisor::ReturnActive(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return;
  }
  auto& info = managers_.at(manager);
  ActiveMinus(info, bytes);

  SanityCheck();
  KillCheck(policy_.get(), active_, standby_);
}

void
katana::MemorySupervisor::ReturnStandby(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return;
  }
  auto& info = managers_.at(manager);
  StandbyMinus(info, bytes);

  SanityCheck();
  KillCheck(policy_.get(), active_, standby_);
}

count_t
katana::MemorySupervisor::ActiveToStandby(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return 0;
  }
  auto& info = managers_.at(manager);
  ActiveMinus(info, bytes);
  StandbyPlus(info, bytes);
  count_t try_reclaim = policy_->ReclaimForMemoryPressure(active_, standby_);
  ReclaimMemory(try_reclaim);

  if (policy_->MemoryPressureHigh(active_, standby_)) {
    SanityCheck();
    KillCheck(policy_.get(), active_, standby_);
    return 0;
  }

  SanityCheck();
  KillCheck(policy_.get(), active_, standby_);
  return bytes;
}

void
katana::MemorySupervisor::StandbyToActive(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return;
  }
  auto& info = managers_.at(manager);
  ActivePlus(info, bytes);
  StandbyMinus(info, bytes);
  count_t try_reclaim = policy_->ReclaimForMemoryPressure(active_, standby_);
  ReclaimMemory(try_reclaim);

  SanityCheck();
  KillCheck(policy_.get(), active_, standby_);
}

void
katana::MemorySupervisor::SetPolicy(std::unique_ptr<MemoryPolicy> policy) {
  policy_.swap(policy);
}

uint64_t
katana::MemorySupervisor::GetTotalSystemMemory() {
  uint64_t pages = sysconf(_SC_PHYS_PAGES);
  uint64_t page_size = sysconf(_SC_PAGE_SIZE);
  return pages * page_size;
}

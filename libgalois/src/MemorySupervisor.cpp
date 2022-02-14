#include "katana/MemorySupervisor.h"

#include <fstream>

#include "katana/Cache.h"
#include "katana/MemoryPolicy.h"
#include "katana/ProgressTracer.h"
#include "katana/PropertyManager.h"
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
  for (auto& [name, info] : managers_) {
    manager_active += info.active;
    manager_standby += info.standby;
  }
  if (manager_active != active_ || manager_standby != standby_) {
    LogState(sanity_str, active_, standby_);
    katana::GetTracer().GetActiveSpan().Log(
        "active/standby mismatch with manager totals",
        {
            {"manager_active", manager_active},
            {"manager_standby", manager_standby},
        });

    KATANA_LOG_WARN(
        "manager active {} manager standby {}", manager_active,
        manager_standby);
  }
  if (active_ < 0) {
    LogState(sanity_str, active_, standby_);
    // TODO(witchel)
    // KATANA_LOG_ASSERT(false);
  }
  if (standby_ < 0) {
    LogState(sanity_str, active_, standby_);
    KATANA_LOG_ASSERT(false);
  }
}

katana::MemorySupervisor::MemorySupervisor() {
  physical_ = GetTotalSystemMemory();
  policy_ = std::make_unique<katana::MemoryPolicyMinimal>();
  // Memory supervisor creates managers
  auto pr = std::make_unique<PropertyManager>();
  const auto& name = pr->Name();
  managers_[name].manager_ = std::move(pr);

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

void
katana::MemorySupervisor::ReclaimMemory(count_t goal) {
  if (goal <= 0) {
    return;
  }
  count_t reclaimed = 0;
  // TODO(witchel) policies should include reclaim in proportion to current use
  for (auto& [name, info] : managers_) {
    // Manager implementation of FreeStandbyMemory calls MemorySupervisor::ReturnStandby
    auto got = info.manager_->FreeStandbyMemory(goal - reclaimed);
    reclaimed += got;
    if (reclaimed >= goal) {
      break;
    }
  }
}

void
katana::MemorySupervisor::BorrowActive(const std::string& name, count_t bytes) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return;
  }
  auto& info = it->second;

  ActivePlus(info, bytes);
  count_t try_reclaim = policy_->ReclaimForMemoryPressure(active_, standby_);
  ReclaimMemory(try_reclaim);

  SanityCheck();
  KillCheck(policy_.get(), active_, standby_);
}

count_t
katana::MemorySupervisor::BorrowStandby(const std::string& name, count_t goal) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return 0;
  }
  auto& info = it->second;

  count_t try_reclaim = policy_->ReclaimForMemoryPressure(active_, standby_);
  ReclaimMemory(try_reclaim);

  if (policy_->MemoryPressureHigh(active_, standby_)) {
    return 0;
  }

  StandbyPlus(info, goal);

  KillCheck(policy_.get(), active_, standby_);
  return std::min(goal, Available());
}

void
katana::MemorySupervisor::ReturnActive(const std::string& name, count_t bytes) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return;
  }
  auto& info = it->second;

  ActiveMinus(info, bytes);

  SanityCheck();
  KillCheck(policy_.get(), active_, standby_);
}

void
katana::MemorySupervisor::ReturnStandby(
    const std::string& name, count_t bytes) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return;
  }
  auto& info = it->second;

  StandbyMinus(info, bytes);

  SanityCheck();
  KillCheck(policy_.get(), active_, standby_);
}

count_t
katana::MemorySupervisor::ActiveToStandby(
    const std::string& name, count_t bytes) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return 0;
  }
  auto& info = it->second;

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
katana::MemorySupervisor::StandbyToActive(
    const std::string& name, count_t bytes) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return;
  }
  auto& info = it->second;

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

katana::CacheStats
katana::MemorySupervisor::GetPropertyCacheStats() const {
  auto name = PropertyManager::name_;
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return katana::CacheStats();
  }
  const auto& info = it->second;
  auto* pm = dynamic_cast<PropertyManager*>(info.manager_.get());
  return pm->GetPropertyCacheStats();
}

katana::PropertyManager*
katana::MemorySupervisor::GetPropertyManager() {
  auto name = PropertyManager::name_;
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return nullptr;
  }
  const auto& info = it->second;
  auto* pm = dynamic_cast<PropertyManager*>(info.manager_.get());
  return pm;
}

uint64_t
katana::MemorySupervisor::GetTotalSystemMemory() {
  uint64_t pages = sysconf(_SC_PHYS_PAGES);
  uint64_t page_size = sysconf(_SC_PAGE_SIZE);
  return pages * page_size;
}

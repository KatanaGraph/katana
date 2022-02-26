#include "katana/MemorySupervisor.h"

#include <fstream>

#include "katana/Cache.h"
#include "katana/MemoryPolicy.h"
#include "katana/PropertyManager.h"
#include "katana/Time.h"

using katana::count_t;

namespace {

const std::string sanity_str = "memory manager sanity";
const std::string oversubscribed_str = "memory manager oversubscribed";
const std::string unregister_str = "memory manager unregister";

void
LogState(const std::string& str, count_t standby, count_t bytes_reclaimed) {
  katana::GetTracer().GetActiveSpan().Log(
      str, {
               {"standby", standby},
               {"reclaimed", bytes_reclaimed},
           });
}

void
KillCheck(
    katana::MemoryPolicy* policy, count_t standby, count_t bytes_reclaimed) {
  if (policy->KillSelfForLackOfMemory(standby)) {
    LogState(oversubscribed_str, standby, bytes_reclaimed);
    KATANA_LOG_FATAL("out of memory");
  }
}

}  // anonymous namespace

void
katana::MemorySupervisor::SanityCheck() {
  count_t manager_standby{};
  for (auto& [name, info] : managers_) {
    manager_standby += info.standby;
  }
  if (manager_standby != standby_) {
    LogState(sanity_str, standby_, bytes_reclaimed_);
    katana::GetTracer().GetActiveSpan().Log(
        "standby mismatch with manager totals",
        {
            {"manager_standby", manager_standby},
        });

    KATANA_LOG_WARN("manager standby {}", manager_standby);
  }
  if (standby_ < 0) {
    LogState(sanity_str, standby_, bytes_reclaimed_);
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
katana::MemorySupervisor::ReclaimMemory(count_t goal) {
  if (goal <= 0) {
    return;
  }
  count_t reclaimed = 0;
  // TODO(witchel) policies should include reclaim in proportion to current use
  for (auto& [name, info] : managers_) {
    // Manager implementation of FreeStandbyMemory calls
    // MemorySupervisor::ReturnStandby, so we see calls into the MemorySupervisor from
    // here.
    auto got = info.manager_->FreeStandbyMemory(goal - reclaimed);
    reclaimed += got;
    if (reclaimed >= goal) {
      break;
    }
  }
  bytes_reclaimed_ += reclaimed;
}

count_t
katana::MemorySupervisor::GetStandby(const std::string& name, count_t goal) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return 0;
  }
  auto& info = it->second;

  CheckPressure();
  if (policy_->IsMemoryPressureHigh(standby_)) {
    return 0;
  }

  StandbyPlus(info, goal);

  SanityCheck();
  KillCheck(policy_.get(), standby_, bytes_reclaimed_);
  return std::min(goal, Available());
}

void
katana::MemorySupervisor::PutStandby(const std::string& name, count_t bytes) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return;
  }
  auto& info = it->second;

  StandbyMinus(info, bytes);

  SanityCheck();
  // No pressure check or kill check because we are reducing memory use and this is
  // probably called in response to ReclaimMemory, above, which will call KillCheck
  // when complete.
}

void
katana::MemorySupervisor::ActiveToStandby(
    const std::string& name, count_t bytes) {
  auto it = managers_.find(name);
  if (it == managers_.end()) {
    KATANA_LOG_WARN("no manager with name {}\n", name);
    return;
  }
  auto& info = it->second;

  StandbyPlus(info, bytes);

  CheckPressure();
  SanityCheck();
  KillCheck(policy_.get(), standby_, bytes_reclaimed_);
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

  StandbyMinus(info, bytes);

  CheckPressure();
  SanityCheck();
  KillCheck(policy_.get(), standby_, bytes_reclaimed_);
}

void
katana::MemorySupervisor::CheckPressure() {
  count_t try_reclaim = policy_->ReclaimForMemoryPressure(standby_);
  ReclaimMemory(try_reclaim);
}

void
katana::MemorySupervisor::SetPolicy(std::unique_ptr<MemoryPolicy> policy) {
  policy_.swap(policy);
  CheckPressure();
  SanityCheck();
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

void
katana::MemorySupervisor::LogMemoryStats(const std::string& message) {
  policy_->LogMemoryStats(message, standby_);
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

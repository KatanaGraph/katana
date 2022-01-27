#include "katana/MemorySupervisor.h"

#include "katana/ProgressTracer.h"
#include "katana/Time.h"

using katana::count_t;

namespace {
uint64_t
getTotalSystemMemory() {
  uint64_t pages = sysconf(_SC_PHYS_PAGES);
  uint64_t page_size = sysconf(_SC_PAGE_SIZE);
  return pages * page_size;
}

const std::string sanity_str = "memory manager sanity";
const std::string oversubscribed_str = "memory manager oversubscribed";
const std::string unregister_str = "memory manager unregister";
}  // anonymous namespace

void
katana::MemorySupervisor::LogState(const std::string& str) {
  katana::GetTracer().GetActiveSpan().Log(
      str, {
               {"active", active_},
               {"standby", standby_},
           });
}

void
katana::MemorySupervisor::Sanity() {
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
    logit = true;
  }
  if (standby_ < 0) {
    logit = true;
  }
  if (logit) {
    LogState(sanity_str);
  }
}

katana::MemorySupervisor::MemorySupervisor() {
  physical_ = getTotalSystemMemory();
  // Let's consider our limit a little conservatively
  auto os_and_overhead = std::min(
      (uint64_t)(0.9 * physical_),
      (uint64_t)2 * ((uint64_t)1024 * (uint64_t)1024 * (uint64_t)1024));
  physical_ -= os_and_overhead;
  auto& tracer = katana::GetTracer();
  tracer.GetActiveSpan().Log(
      "memory manager",
      {{"physical", physical_},
       {"physical_human", katana::BytesToStr("{:.2f}{}", physical_)}});
  // /sys/fs/cgroup/memory
  //  Open memory.oom_control for reading.
  //Create a file descriptor for notification by doing eventfd(0, 0).
  //Write "<fd of open()> <fd of eventfd()>" to cgroup.event_control.
  //The process would then do something like:

  //    uint64_t ret;
  //    read(<fd of eventfd()>, &ret, sizeof(ret));
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
    LogState(unregister_str);
  }
  ActiveMinus(info, info.active);
  StandbyMinus(info, info.standby);
  managers_.erase(manager);
}

void
katana::MemorySupervisor::ReclaimMemory(count_t goal) {
  count_t reclaimed = 0;
  // TODO(witchel) reclaim in proportion to current use
  for (auto& [manager, info] : managers_) {
    auto got = manager->FreeStandbyMemory(goal - reclaimed);
    StandbyMinus(info, got);
    reclaimed += got;
    if (reclaimed >= goal)
      break;
  }
}

void
katana::MemorySupervisor::BorrowActive(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return;
  }
  auto& info = managers_.at(manager);
  ActivePlus(info, bytes);
  if (MemoryOversubscribed()) {
    ReclaimMemory(bytes);
    if (MemoryOversubscribed()) {
      // TODO (witchel) explore policies where we kill ourselves before OOM
      LogState(oversubscribed_str);
    }
  }
}

count_t
katana::MemorySupervisor::BorrowStandby(Manager* manager, count_t goal) {
  if (!CheckRegistered(manager)) {
    return 0;
  }
  if (MemoryOversubscribed()) {
    return 0;
  }
  auto& info = managers_.at(manager);
  // TODO (witchel) explore policies where managers are allowed to eat into each
  // other's standby memory, if the current allocation is unfair
  auto grant = std::min(goal, Available());
  StandbyPlus(info, grant);
  return grant;
}

void
katana::MemorySupervisor::ReturnActive(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return;
  }
  auto& info = managers_.at(manager);
  ActiveMinus(info, bytes);
}

void
katana::MemorySupervisor::ReturnStandby(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return;
  }
  auto& info = managers_.at(manager);
  StandbyMinus(info, bytes);
}

count_t
katana::MemorySupervisor::ActiveToStandby(Manager* manager, count_t bytes) {
  if (!CheckRegistered(manager)) {
    return 0;
  }
  auto& info = managers_.at(manager);
  ActiveMinus(info, bytes);
  if (MemoryOversubscribed()) {
    auto grant = std::min(bytes, Available());
    StandbyPlus(info, grant);
    return grant;
  }
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
}

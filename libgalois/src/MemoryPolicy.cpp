#include "katana/MemoryPolicy.h"

#include <fstream>

#include "katana/MemoryPolicy.h"
#include "katana/MemorySupervisor.h"
#include "katana/ProgressTracer.h"
#include "katana/Time.h"

// anchor vtable
katana::MemoryPolicy::~MemoryPolicy() = default;

struct katana::MemoryPolicy::MemInfo {
  count_t active;
  count_t standby;
  uint64_t rss_bytes;
  double used_ratio;
  int64_t oom_score;
};

using katana::count_t;

namespace {

void
LogIt(const std::string& str, katana::MemoryPolicy::MemInfo* mem_info) {
  auto scope = katana::GetTracer().StartActiveSpan(str);
  scope.span().Log(
      "mem_stats", {
                       {"rss_gb", katana::ToGB(mem_info->rss_bytes)},
                       {"oom_score", mem_info->oom_score},
                       {"used_ratio", mem_info->used_ratio},
                       {"active_gb", katana::ToGB(mem_info->active)},
                       {"standby_gb", katana::ToGB(mem_info->standby)},
                   });
}

}  // namespace

void
katana::MemoryPolicy::UpdateMemInfo(
    MemInfo* mem_info, count_t active, count_t standby) const {
  mem_info->active = active;
  mem_info->standby = standby;
  mem_info->rss_bytes = katana::ProgressTracer::ParseProcSelfRssBytes();
  mem_info->used_ratio = (double)mem_info->rss_bytes / physical();
  mem_info->oom_score = OOMScore();
}

///////////////////////////////////////////////////////////////////////////
// MemoryPolicyPerformance

bool
katana::MemoryPolicyPerformance::MemoryPressureHigh(
    count_t active, count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, active, standby);

  if (mem_info.oom_score > 1100 ||
      mem_info.used_ratio > high_used_ratio_threshold()) {
    LogIt("memory pressure high", &mem_info);
    return true;
  }

  return false;
}

count_t
katana::MemoryPolicyPerformance::ReclaimForMemoryPressure(
    count_t active, count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, active, standby);

  if (mem_info.oom_score < 700 ||
      mem_info.used_ratio < high_used_ratio_threshold()) {
    return 0;
  }
  if (mem_info.oom_score < 850) {
    LogIt(
        fmt::format(
            "reclaim for memory pressure {} GB", katana::ToGB(standby / 4)),
        &mem_info);
    return standby / 4;
  }
  if (mem_info.oom_score < 1000) {
    LogIt(
        fmt::format(
            "reclaim for memory pressure {} GB", katana::ToGB(standby / 2)),
        &mem_info);
    return standby / 2;
  }
  LogIt(
      fmt::format("reclaim for memory pressure {} GB", katana::ToGB(standby)),
      &mem_info);
  return standby;
}

bool
katana::MemoryPolicyPerformance::KillSelfForLackOfMemory(
    count_t active, count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, active, standby);

  if (mem_info.oom_score > 1200 ||
      mem_info.used_ratio > kill_used_ratio_threshold()) {
    LogIt("KILL SELF", &mem_info);
    return true;
  }
  return false;
}

katana::MemoryPolicyPerformance::MemoryPolicyPerformance()
    : MemoryPolicy(
          {.high_used_ratio_threshold = 0.85,
           .kill_used_ratio_threshold = 0.93}) {}

//////////////////////////////////////////////////////////////////////
// MemoryPolicyMinimal
bool
katana::MemoryPolicyMinimal::MemoryPressureHigh(
    count_t active, count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, active, standby);
  if (mem_info.oom_score > 1100 ||
      mem_info.used_ratio > high_used_ratio_threshold()) {
    LogIt("memory pressure high", &mem_info);
    return true;
  }

  return false;
}

count_t
katana::MemoryPolicyMinimal::ReclaimForMemoryPressure(
    count_t active, count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, active, standby);

  if (MemoryPressureHigh(active, standby)) {
    return standby;
  }
  return 0;
}

bool
katana::MemoryPolicyMinimal::KillSelfForLackOfMemory(
    count_t active, count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, active, standby);

  if (mem_info.oom_score > 1200 ||
      mem_info.used_ratio > kill_used_ratio_threshold()) {
    LogIt("KILL SELF", &mem_info);
    return true;
  }
  return false;
}

katana::MemoryPolicyMinimal::MemoryPolicyMinimal()
    : MemoryPolicy(
          {.high_used_ratio_threshold = 0.95,
           .kill_used_ratio_threshold = 0.95}) {}

//////////////////////////////////////////////////////////////////////
// MemoryPolicy

katana::MemoryPolicy::MemoryPolicy(
    katana::MemoryPolicy::Thresholds thresholds) {
  physical_ = katana::MemorySupervisor::GetTotalSystemMemory();
  // We divide by physical_
  if (physical_ == 0) {
    physical_ = 1;
  }
  thresholds_ = thresholds;
}

#if __linux__

// TODO(witchel), check /proc/self/oom_adj and /proc/self/oom_score_adj
uint64_t
katana::MemoryPolicy::OOMScore() {
  std::ifstream proc_self("/proc/self/oom_score");

  if (!proc_self) {
    KATANA_LOG_WARN(
        "cannot open /proc/self/oom_score: {}", std::strerror(errno));
    return 0;
  }
  std::string line;
  std::getline(proc_self, line);
  uint64_t value{};
  try {
    value = static_cast<uint64_t>(std::stoull(line, nullptr, 0));
  } catch (std::exception& e) {
    KATANA_LOG_WARN(
        "problem parsing output of /proc/self/oom_score: {}", e.what());
  }
  return value;
}

#else

uint64_t
katana::MemoryPolicy::OOMScore() {
  KATANA_WARN_ONCE("Platform does not have out of memory (OOM) scoring");
  return 0;
}

#endif

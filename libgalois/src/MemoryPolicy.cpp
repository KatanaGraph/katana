#include "katana/MemoryPolicy.h"

#include <fstream>
#include <regex>

#include "katana/MemoryPolicy.h"
#include "katana/MemorySupervisor.h"
#include "katana/ProgressTracer.h"
#include "katana/Time.h"

// anchor vtable
katana::MemoryPolicy::~MemoryPolicy() = default;

struct katana::MemoryPolicy::MemInfo {
  count_t standby;
  count_t rss_bytes;
  count_t available_bytes;
  double used_ratio;
  int64_t oom_score;
};

using katana::count_t;

namespace {

void
LogIt(const std::string& str, katana::MemoryPolicy::MemInfo* mem_info) {
  katana::GetTracer().GetActiveSpan().Log(
      str, {
               {"rss_gb", katana::ToGB(mem_info->rss_bytes)},
               {"available_gb", katana::ToGB(mem_info->available_bytes)},
               {"oom_score", mem_info->oom_score},
               {"used_ratio", mem_info->used_ratio},
               {"standby_gb", katana::ToGB(mem_info->standby)},
           });
}

}  // namespace

void
katana::MemoryPolicy::LogMemoryStats(
    const std::string& message, count_t standby) {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);
  LogIt(message, &mem_info);
}

void
katana::MemoryPolicy::UpdateMemInfo(MemInfo* mem_info, count_t standby) const {
  mem_info->standby = standby;
  mem_info->rss_bytes = katana::ProgressTracer::ParseProcSelfRssBytes();
  mem_info->available_bytes = AvailableMemoryBytes();
  mem_info->used_ratio = (double)mem_info->rss_bytes / physical();
  mem_info->oom_score = OOMScore();
}

///////////////////////////////////////////////////////////////////////////
// MemoryPolicyPerformance

bool
katana::MemoryPolicyPerformance::IsMemoryPressureHigh(count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);

  if ((mem_info.oom_score > high_pressure_oom_threshold() ||
       mem_info.used_ratio > high_used_ratio_threshold()) &&
      mem_info.available_bytes < 0.1 * physical()) {
    LogIt("memory pressure high", &mem_info);
    return true;
  }

  return false;
}

count_t
katana::MemoryPolicyPerformance::ReclaimForMemoryPressure(
    count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);
  count_t reclaim = standby;

  if (mem_info.oom_score < 1000 ||
      mem_info.used_ratio < high_used_ratio_threshold() ||
      mem_info.available_bytes > 0.1 * physical()) {
    return 0;
  }
  // TODO (witchel) this might give back memory too quickly
  if (mem_info.oom_score < 1000) {
    reclaim = standby / 4;
  } else if (mem_info.oom_score < 1200) {
    reclaim = standby / 2;
  }
  LogIt(
      fmt::format("reclaim for memory pressure {} GB", katana::ToGB(reclaim)),
      &mem_info);
  return reclaim;
}

bool
katana::MemoryPolicyPerformance::KillSelfForLackOfMemory(
    count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);

  if ((mem_info.oom_score > kill_self_oom_threshold() ||
       mem_info.used_ratio > kill_used_ratio_threshold()) &&
      mem_info.available_bytes < 0.1 * physical()) {
    LogIt("KILL SELF", &mem_info);
    return true;
  }
  return false;
}

katana::MemoryPolicyPerformance::MemoryPolicyPerformance()
    : MemoryPolicy({
          .high_used_ratio_threshold = 0.85,
          .kill_used_ratio_threshold = 0.95,
          .kill_self_oom_threshold = 1280,
          .high_pressure_oom_threshold = 1100,
      }) {}

//////////////////////////////////////////////////////////////////////
// MemoryPolicyMinimal
bool
katana::MemoryPolicyMinimal::IsMemoryPressureHigh(count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);
  if (mem_info.oom_score > high_pressure_oom_threshold() ||
      mem_info.used_ratio > high_used_ratio_threshold()) {
    LogIt("memory pressure high", &mem_info);
    return true;
  }

  return false;
}

count_t
katana::MemoryPolicyMinimal::ReclaimForMemoryPressure(count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);

  if (IsMemoryPressureHigh(standby)) {
    return standby;
  }
  return 0;
}

bool
katana::MemoryPolicyMinimal::KillSelfForLackOfMemory(count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);

  if (mem_info.oom_score > kill_self_oom_threshold() ||
      mem_info.used_ratio > kill_used_ratio_threshold()) {
    LogIt("KILL SELF", &mem_info);
    return true;
  }
  return false;
}

katana::MemoryPolicyMinimal::MemoryPolicyMinimal()
    : MemoryPolicy({
          .high_used_ratio_threshold = 0.95,
          .kill_used_ratio_threshold = 0.95,
          .kill_self_oom_threshold = 1280,
          .high_pressure_oom_threshold = 1100,
      }) {}

//////////////////////////////////////////////////////////////////////
// MemoryPolicyMeek
bool
katana::MemoryPolicyMeek::IsMemoryPressureHigh(count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);
  if ((mem_info.oom_score > high_pressure_oom_threshold() ||
       mem_info.used_ratio > high_used_ratio_threshold()) &&
      mem_info.available_bytes < 0.1 * physical()) {
    LogIt("memory pressure high", &mem_info);
    return true;
  }

  return false;
}

count_t
katana::MemoryPolicyMeek::ReclaimForMemoryPressure(count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);

  if (mem_info.available_bytes < 0.1 * physical()) {
    return standby;
  }
  return 0;
}

bool
katana::MemoryPolicyMeek::KillSelfForLackOfMemory(count_t standby) const {
  MemInfo mem_info;
  UpdateMemInfo(&mem_info, standby);

  if ((mem_info.oom_score > kill_self_oom_threshold() ||
       mem_info.used_ratio > kill_used_ratio_threshold()) &&
      mem_info.available_bytes < 0.1 * physical()) {
    LogIt("KILL SELF", &mem_info);
    return true;
  }
  return false;
}

katana::MemoryPolicyMeek::MemoryPolicyMeek()
    : MemoryPolicy({
          .high_used_ratio_threshold = 0.85,
          .kill_used_ratio_threshold = 0.95,
          .kill_self_oom_threshold = 1280,
          .high_pressure_oom_threshold = 1100,
      }) {}

//////////////////////////////////////////////////////////////////////
// MemoryPolicyNull
bool
katana::MemoryPolicyNull::IsMemoryPressureHigh(
    [[maybe_unused]] count_t standby) const {
  return false;
}

count_t
katana::MemoryPolicyNull::ReclaimForMemoryPressure(
    [[maybe_unused]] count_t standby) const {
  return 0;
}

bool
katana::MemoryPolicyNull::KillSelfForLackOfMemory(
    [[maybe_unused]] count_t standby) const {
  return false;
}

katana::MemoryPolicyNull::MemoryPolicyNull()
    : MemoryPolicy({
          .high_used_ratio_threshold = 0.85,
          .kill_used_ratio_threshold = 0.95,
          .kill_self_oom_threshold = 1280,
          .high_pressure_oom_threshold = 1100,
      }) {}

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

uint64_t
katana::MemoryPolicy::AvailableMemoryBytes() {
  static std::regex kFreeRegex("^MemAvailable:\\s*([0-9]+) kB");
  std::ifstream proc_self("/proc/meminfo");

  if (!proc_self) {
    KATANA_LOG_WARN("cannot open /proc/meminfo: {}", std::strerror(errno));
    return 0;
  }
  uint64_t value{};
  std::string line;
  while (std::getline(proc_self, line)) {
    std::smatch sub_match;
    if (!std::regex_match(line, sub_match, kFreeRegex)) {
      continue;
    }
    std::string val = sub_match[1];
    value = static_cast<uint64_t>(std::atoll(val.c_str()));
  }

  return value * 1024;
}

#else

uint64_t
katana::MemoryPolicy::OOMScore() {
  KATANA_WARN_ONCE("Platform does not have out of memory (OOM) scoring");
  return 0;
}

#endif

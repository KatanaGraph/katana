#include "katana/OSMemoryNotify.h"

#include <fstream>
#include <regex>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/config.h"

#if __linux__
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>
//#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#endif

#if __linux__

namespace {
std::string
GetMemoryCGroupRoot() {
  static std::regex kMemoryRegex(".*:memory:(.+)");
  std::ifstream proc_self("/proc/self/cgroup");

  std::string line;
  while (std::getline(proc_self, line)) {
    std::smatch sub_match;
    fmt::print("{}\n", line);
    if (!std::regex_match(line, sub_match, kMemoryRegex)) {
      continue;
    }
    fmt::print("match ({}) ({})\n", sub_match[0], sub_match[1]);
    std::string val = sub_match[1];
    if (val.empty()) {
      KATANA_LOG_WARN("empty memory cgroup");
    } else if (val[0] != '/') {
      KATANA_LOG_WARN("memory cgroup does not start with / ({})", val);
    }
    return val;
  }
  return "";
}

void
ListenToOS(int event_fd) {
  fmt::print("eeeeeeeeeeefd {}\n", event_fd);
  while (true) {
    uint64_t x;
    int ret = read(event_fd, &x, sizeof(x));

    fmt::print("Listen returned ! {}, ret {}\n", x, ret);
    if (ret == -1 && (errno == EINTR || errno == EAGAIN)) {
      continue;
    }
    if (ret != sizeof(x)) {
      KATANA_LOG_WARN("problem reading event fd: {}", strerror(errno));
    }

    fmt::print("GOT {}\n", x);
  }
}

katana::Result<std::future<void>>
InitializeEventFD(const std::string& memory_cgroup_root) {
  int event_fd = eventfd(0, 0);
  if (event_fd == -1) {
    return KATANA_ERROR(
        katana::ErrorCode::OSError, "eventfd failed: {}", strerror(errno));
  }
  fmt::print("eventfd {}\n", event_fd);

  std::string sysfs_root = "/sys/fs/cgroup/memory";
  fmt::print("CGRPU ROOT {}\n", memory_cgroup_root);
  // memory.pressure_level does has permissions of 0000
  // int memory_pressure_fd{};
  // {
  //   std::string mp_path =
  //       sysfs_root + memory_cgroup_root + "/memory.pressure_level";
  //   fmt::print("mp_path {}\n", mp_path);
  //   memory_pressure_fd = open(mp_path.c_str(), O_RDONLY);
  //   if (memory_pressure_fd == -1) {
  //     return KATANA_ERROR(
  //         katana::ErrorCode::OSError, "opening memory.pressure_level: {}",
  //         strerror(errno));
  //   }
  // }

  int memory_pressure_fd{};
  {
    std::string mp_path =
        sysfs_root + memory_cgroup_root + "/memory.oom_control";
    fmt::print("mp_path {}\n", mp_path);
    memory_pressure_fd = open(mp_path.c_str(), O_RDONLY);
    if (memory_pressure_fd == -1) {
      return KATANA_ERROR(
          katana::ErrorCode::OSError, "opening memory.pressure_level: {}",
          strerror(errno));
    }
  }

  fmt::print("mem fd {}\n", memory_pressure_fd);

  int event_control_fd{};
  {
    std::string event_control_path =
        sysfs_root + memory_cgroup_root + "/cgroup.event_control";
    fmt::print("event contro path {}\n", event_control_path);
    event_control_fd = open(event_control_path.c_str(), O_WRONLY);
    if (event_control_fd == -1) {
      return KATANA_ERROR(
          katana::ErrorCode::OSError, "opening cgroup.even_control: {}",
          strerror(errno));
    }
  }
  fmt::print("event control fd {}\n", event_control_fd);

  {
    // Get local notifications of medium memory pressure.
    // Medium is swapping, high means OOM is coming soon
    // https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v1/memory.html#memory-pressure
    std::string config_msg = fmt::format("{} {}", event_fd, memory_pressure_fd);
    // +1 for null
    size_t ret = static_cast<size_t>(
        write(event_control_fd, config_msg.c_str(), config_msg.size() + 1));
    if (ret != (config_msg.size() + 1)) {
      KATANA_LOG_WARN(
          "problem configuring memory pressure cgroup: {}", strerror(errno));
    }
  }

  // Once we write event_control_fd, we don't need these FUDs
  close(event_control_fd);
  //close(memory_pressure_fd);

  // Spawn OS thread to listen to the eventfd
  return std::async(std::launch::async, &ListenToOS, event_fd);
}

}  // anonymous namespace

katana::OSMemoryNotify::OSMemoryNotify() {
  memory_cgroup_root_ = GetMemoryCGroupRoot();
  auto res = InitializeEventFD(memory_cgroup_root_);
  if (!res) {
    KATANA_LOG_WARN("problem initializing eventfd: {}", res.error());
  } else {
    eventfd_thread_ = std::move(res.value());
    //eventfd_thread_.get();
  }
}

#else

katana::OSMemoryNotify::OSMemoryNotify() {
  KATANA_WARN_ONCE("no OS memory notify mechanism on this platform");
}

#endif

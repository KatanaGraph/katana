#pragma once

#include <future>
#include <string>

#include "katana/config.h"

namespace katana {
/// This class allows processes to get notified by the Linux kernel about memory usage.
///

class KATANA_EXPORT OSMemoryNotify {
public:
  OSMemoryNotify();

private:
  std::string memory_cgroup_root_;
  std::future<void> eventfd_thread_;
};

}  // namespace katana

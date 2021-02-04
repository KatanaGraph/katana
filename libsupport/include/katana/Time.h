#ifndef KATANA_LIBSUPPORT_KATANA_TIME_H_
#define KATANA_LIBSUPPORT_KATANA_TIME_H_

#include <chrono>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"

namespace katana {

using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;

inline TimePoint
Now() {
  return Clock::now();
}

uint64_t
UsSince(const TimePoint& point) {
  using Us = std::chrono::microseconds;
  return std::chrono::duration_cast<Us>(Now() - point).count();
}
uint64_t
UsBetween(const TimePoint& before, const TimePoint& after) {
  using Us = std::chrono::microseconds;
  return std::chrono::duration_cast<Us>(after - before).count();
}

inline std::pair<float, std::string>
UsToPair(uint64_t us_) {
  auto us = static_cast<float>(us_);
  static std::vector<std::string> suffixes = {"us", "ms", "s"};
  for (auto const& suffix : suffixes) {
    if (us < 1000) {
      return std::pair(us, suffix);
    }
    us /= 1000;
  }
  return std::pair(us, "s");
}

inline std::string
UsToStr(const std::string& fmt, uint64_t us_) {
  auto [val, unit] = UsToPair(us_);
  return fmt::format(fmt, val, unit);
}

// Input: Byte count
// Output: scaled count and units
inline std::string
BytesToStr(const std::string& fmt, uint64_t bytes_) {
  static std::vector<std::string> suffixes = {"B",  "KB", "MB",
                                              "GB", "TB", "PB"};
  auto bytes = static_cast<float>(bytes_);
  for (auto const& suffix : suffixes) {
    if (bytes < 1024.0) {
      return fmt::format(fmt, bytes, suffix);
    }
    bytes /= 1024;
  }
  return fmt::format(fmt, bytes, "PB");
}

}  // namespace katana
#endif

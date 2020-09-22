#ifndef GALOIS_LIBTSUBA_BENCHUTILS_H_
#define GALOIS_LIBTSUBA_BENCHUTILS_H_

#include <cmath>
#include <ctime>
#include <numeric>

#include "galois/Logging.h"
#include "galois/Result.h"

inline struct timespec
now() {
  struct timespec tp;
  // CLOCK_BOOTTIME is probably better, but Linux specific
  int ret = clock_gettime(CLOCK_MONOTONIC, &tp);
  if (ret < 0) {
    GALOIS_LOG_ERROR("clock_gettime {}", galois::ResultErrno().message());
  }
  return tp;
}

inline struct timespec
timespec_sub(struct timespec time, struct timespec oldTime) {
  if (time.tv_nsec < oldTime.tv_nsec)
    return (struct timespec){
        .tv_sec = time.tv_sec - 1 - oldTime.tv_sec,
        .tv_nsec = 1'000'000'000L + time.tv_nsec - oldTime.tv_nsec};
  else
    return (struct timespec){
        .tv_sec = time.tv_sec - oldTime.tv_sec,
        .tv_nsec = time.tv_nsec - oldTime.tv_nsec};
}

inline int64_t
timespec_to_us(struct timespec ts) {
  return ts.tv_sec * 1'000'000 + ts.tv_nsec / 1'000;  // '
}

// Input: microseconds
// Output: scaled time and units
inline std::pair<float, std::string>
UsToPair(uint64_t us_) {
  float us = (float)us_;
  static std::vector<std::string> suffixes = {"us", "ms", "s"};
  for (auto const& suffix : suffixes) {
    if (us < 1000) {
      return std::make_pair(us, suffix);
    }
    us /= 1000;
  }
  return std::make_pair(us, "s");
}

// Input: Byte count
// Output: scaled count and units
inline std::pair<float, std::string>
BytesToPair(uint64_t bytes_) {
  static std::vector<std::string> suffixes = {"B",  "KB", "MB",
                                              "GB", "TB", "PB"};
  float bytes = (float)bytes_;
  for (auto const& suffix : suffixes) {
    if (bytes < 1024.0) {
      return std::make_pair(bytes, suffix);
    }
    bytes /= 1024;
  }
  return std::make_pair(bytes, "PB");
}

// Input: vector of timings, byte size of experiment
// Output: string summarizing experiment
inline std::string
FmtResults(const std::vector<int64_t>& v, uint64_t bytes = UINT64_C(0)) {
  if (v.size() == 0) {
    return "no results";
  }
  int64_t sum = std::accumulate(v.begin(), v.end(), 0L);
  double mean = (double)sum / v.size();

  double accum = 0.0;
  std::for_each(std::begin(v), std::end(v), [&](const double d) {
    accum += (d - mean) * (d - mean);
  });
  double stdev = 0.0;
  if (v.size() > 1) {
    stdev = sqrt(accum / (v.size() - 1));
  }

  auto [time, time_units] = UsToPair(mean);
  float stdev_in_units = stdev * time / mean;
  if (bytes && mean) {
    auto [bw, bw_units] = BytesToPair(1'000'000 * bytes / mean);
    return fmt::format(
        "{:>5.1f} {:2} (N={:d}) sd {:5.1f} {:5.1f} {}/s", time, time_units,
        v.size(), stdev_in_units, bw, bw_units);
  }
  return fmt::format(
      "{:>5.1f} {:2} (N={:d}) sd {:5.1f}", time, time_units, v.size(),
      stdev_in_units);
}

#endif

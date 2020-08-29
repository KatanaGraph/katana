#ifndef GALOIS_LIBTSUBA_BENCHUTILS_H_
#define GALOIS_LIBTSUBA_BENCHUTILS_H_

#include <time.h>
#include "galois/Result.h"

inline struct timespec now() {
  struct timespec tp;
  // CLOCK_BOOTTIME is probably better, but Linux specific
  int ret = clock_gettime(CLOCK_MONOTONIC, &tp);
  if (ret < 0) {
    GALOIS_LOG_ERROR("clock_gettime {}", galois::ResultErrno().message());
  }
  return tp;
}

inline struct timespec timespec_sub(struct timespec time,
                                    struct timespec oldTime) {
  if (time.tv_nsec < oldTime.tv_nsec)
    return (struct timespec){.tv_sec  = time.tv_sec - 1 - oldTime.tv_sec,
                             .tv_nsec = 1'000'000'000L + time.tv_nsec -
                                        oldTime.tv_nsec};
  else
    return (struct timespec){.tv_sec  = time.tv_sec - oldTime.tv_sec,
                             .tv_nsec = time.tv_nsec - oldTime.tv_nsec};
}

inline int64_t timespec_to_us(struct timespec ts) {
  return ts.tv_sec * 1'000'000 + ts.tv_nsec / 1'000;
}

inline std::string UsToString(uint64_t us_) {
  float us                                 = (float)us_;
  static std::vector<std::string> suffixes = {"us", "ms", "s"};
  for (auto const& suffix : suffixes) {
    if (us < 1000) {
      return fmt::format("{:.1f} {}", us, suffix);
    }
    us /= 1000;
  }
  return fmt::format("{:.1f} s", us);
}

inline std::pair<float,std::string> BytesToString(uint64_t bytes_) {
  static std::vector<std::string> suffixes = {"B",  "KB", "MB",
                                              "GB", "TB", "PB"};
  float bytes                              = (float)bytes_;
  for (auto const& suffix : suffixes) {
    if (bytes < 1024.0) {
      return std::make_pair(bytes, suffix);
    }
    bytes /= 1024;
  }
  return std::make_pair(bytes, "PB");
}

#endif

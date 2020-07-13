#ifndef GALOIS_LIBTSUBA_TESTS_BENCH_UTILS_H_
#define GALOIS_LIBTSUBA_TESTS_BENCH_UTILS_H_

#include <time.h>

inline struct timespec now() {
  struct timespec tp;
  // CLOCK_BOOTTIME is probably better, but Linux specific
  int ret = clock_gettime(CLOCK_MONOTONIC, &tp);
  if (ret < 0) {
    perror("clock_gettime");
    GALOIS_LOG_ERROR("Bad return");
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

#endif // GALOIS_LIBTSUBA_TESTS_BENCH_UTILS_H_

#ifndef GALOIS_LIBGALOIS_GALOIS_THREADTIMER_H_
#define GALOIS_LIBGALOIS_GALOIS_THREADTIMER_H_

#include "galois/config.h"
#include "galois/substrate/PerThreadStorage.h"

namespace galois {

class GALOIS_EXPORT ThreadTimer {
  timespec start_;
  timespec stop_;
  uint64_t nsec_{0};

public:
  ThreadTimer() = default;

  void start();

  void stop();

  uint64_t get_nsec() const { return nsec_; }

  uint64_t get_sec() const { return (nsec_ / 1000000000); }

  uint64_t get_msec() const { return (nsec_ / 1000000); }
};

class GALOIS_EXPORT ThreadTimers {
protected:
  substrate::PerThreadStorage<ThreadTimer> timers_;

  void reportTimes(const char* category, const char* region);
};

template <bool enabled>
class PerThreadTimer : private ThreadTimers {
  const char* const region_;
  const char* const category_;

  void reportTimes() { reportTimes(category_, region_); }

public:
  PerThreadTimer(const char* const region, const char* const category)
      : region_(region), category_(category) {}

  PerThreadTimer(const PerThreadTimer&) = delete;
  PerThreadTimer(PerThreadTimer&&) = delete;
  PerThreadTimer& operator=(const PerThreadTimer&) = delete;
  PerThreadTimer& operator=(PerThreadTimer&&) = delete;

  ~PerThreadTimer() { reportTimes(); }

  void start() { timers_.getLocal()->start(); }

  void stop() { timers_.getLocal()->stop(); }
};

template <>
class PerThreadTimer<false> {
public:
  PerThreadTimer(const char* const, const char* const) {}

  PerThreadTimer(const PerThreadTimer&) = delete;
  PerThreadTimer(PerThreadTimer&&) = delete;
  PerThreadTimer& operator=(const PerThreadTimer&) = delete;
  PerThreadTimer& operator=(PerThreadTimer&&) = delete;

  ~PerThreadTimer() = default;

  void start() const {}

  void stop() const {}
};

}  // end namespace galois

#endif

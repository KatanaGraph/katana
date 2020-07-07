#ifndef GALOIS_LIBSUPPORT_GALOIS_COMM_BACKEND_H_
#define GALOIS_LIBSUPPORT_GALOIS_COMM_BACKEND_H_

#include <cstdint>

namespace galois {

struct CommBackend {
  // TODO(thunt) these names are chosen because of NetworkInterface changing
  // them is very disruptive so I'll defer for a time in the future where we're
  // not worried about upstream and can global replace.
  /// The number of tasks involved
  uint32_t Num{1};
  /// The id number of this task
  uint32_t ID{0};
  CommBackend()                         = default;
  CommBackend(const CommBackend& other) = default;
  CommBackend(CommBackend&& other)      = default;
  CommBackend& operator=(const CommBackend& other) = default;
  CommBackend& operator=(CommBackend&& other) = default;
  virtual ~CommBackend()                      = default;
  /// Wait for all tasks to call Barrier
  virtual void Barrier() = 0;
  /// Notify other tasks that there was a failure; e.g., with MPI_Abort
  virtual void NotifyFailure() = 0;
};

struct NullCommBackend : public CommBackend {
  void Barrier() override {}
  void NotifyFailure() override {}
};

} // namespace galois

#endif

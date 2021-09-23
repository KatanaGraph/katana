#ifndef KATANA_LIBSUPPORT_KATANA_COMMBACKEND_H_
#define KATANA_LIBSUPPORT_KATANA_COMMBACKEND_H_

#include <cstdint>
#include <string>

#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT CommBackend {
public:
  CommBackend() = default;
  CommBackend(const CommBackend& other) = delete;
  CommBackend(CommBackend&& other) = delete;
  CommBackend& operator=(const CommBackend& other) = delete;
  CommBackend& operator=(CommBackend&& other) = delete;
  virtual ~CommBackend();

  /// Wait for all tasks to call Barrier
  virtual void Barrier() = 0;
  /// Broadcast bool to everyone
  virtual bool Broadcast(uint32_t root, bool val) = 0;
  /// Broadcast a string of at most max_size to everyone
  virtual std::string Broadcast(
      uint32_t root, const std::string& val, uint64_t max_size) = 0;
  /// Notify other tasks that there was a failure; e.g., with MPI_Abort
  virtual void NotifyFailure() = 0;

  // TODO(thunt): Num and ID were chosen because of NetworkInterface. Changing
  // them is very disruptive so I'll defer for a time in the future where we're
  // not worried about upstream and can global replace.

  /// The number of tasks involved
  uint32_t Num{1};
  /// The id number of this task
  uint32_t ID{0};
  /// The local rank of this task (process ordinal number within within its machine)
  uint32_t LocalRank;
};

class KATANA_EXPORT NullCommBackend : public CommBackend {
public:
  void Barrier() override {}
  void NotifyFailure() override;
  bool Broadcast([[maybe_unused]] uint32_t root, bool val) override {
    return val;
  };
  std::string Broadcast(
      [[maybe_unused]] uint32_t root, const std::string& val,
      uint64_t max_size) override {
    return val.substr(0, max_size);
  }
};

}  // namespace katana

#endif

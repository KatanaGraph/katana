#ifndef KATANA_LIBTSUBA_KATANA_ASYNCOPGROUP_H_
#define KATANA_LIBTSUBA_KATANA_ASYNCOPGROUP_H_

#include <future>
#include <list>

#include "katana/Result.h"

namespace katana {

class AsyncOpGroup {
public:
  struct AsyncOp {
    std::future<katana::CopyableResult<void>> result;
    std::string location;
    std::function<katana::CopyableResult<void>()> on_complete;
  };

  /// Add future to the list of futures this descriptor will wait for, note
  /// the file name for debugging.
  void AddOp(
      std::future<katana::CopyableResult<void>> future, std::string file,
      const std::function<katana::CopyableResult<void>()>& on_complete);

  /// Wait until all operations this descriptor knows about have completed
  katana::Result<void> Finish();
  /// wait for the op at the head of the list, return true if there was one
  bool FinishOne();

private:
  std::list<AsyncOp> pending_ops_;
  uint64_t errors_{0};
  uint64_t total_{0};
  katana::CopyableErrorInfo last_error_;
};

}  // namespace katana

#endif

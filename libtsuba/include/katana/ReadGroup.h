#ifndef KATANA_LIBTSUBA_KATANA_READGROUP_H_
#define KATANA_LIBTSUBA_KATANA_READGROUP_H_

#include <future>
#include <list>
#include <memory>

#include "katana/AsyncOpGroup.h"
#include "katana/Result.h"

namespace katana {

/// Track multiple, outstanding async writes and provide a mechanism to ensure
/// that they have all completed
class KATANA_EXPORT ReadGroup {
public:
  static katana::Result<std::unique_ptr<ReadGroup>> Make();

  /// Wait until all operations this descriptor knows about have completed
  katana::Result<void> Finish();

  /// Add future to the list of futures this ReadGroup will wait for, note
  /// the file name for debugging. `on_complete` is guaranteed to be called
  /// in FIFO order
  void AddOp(
      std::future<CopyableResult<void>> future, const URI& file,
      const std::function<CopyableResult<void>()>& on_complete);

  /// same as AddOp, but the future may return a data type which can then be
  /// consumed by on_complete
  template <typename RetType>
  void AddReturnsOp(
      std::future<CopyableResult<RetType>> future, const URI& file,
      const std::function<CopyableResult<void>(RetType)>& on_complete) {
    // n.b., make shared instead of unique because move capture below prevents
    // passing generic_complete_fn as a std::function
    auto ret_val = std::make_shared<RetType>();
    auto new_future = std::async(
        std::launch::deferred,
        [future = std::move(future), &ret_val_storage = *ret_val]() mutable
        -> katana::CopyableResult<void> {
          auto res = future.get();
          if (!res) {
            return res.error();
          }
          ret_val_storage = res.value();
          return katana::CopyableResultSuccess();
        });

    std::function<katana::CopyableResult<void>()> generic_complete_fn =
        [ret_val, on_complete]() -> katana::CopyableResult<void> {
      return on_complete(std::move(*ret_val));
    };
    AddOp(std::move(new_future), file, generic_complete_fn);
  }

private:
  AsyncOpGroup async_op_group_;
};

}  // namespace katana

#endif

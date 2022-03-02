#ifndef KATANA_LIBGLUON_KATANA_PREPAREDUPDATE_H_
#define KATANA_LIBGLUON_KATANA_PREPAREDUPDATE_H_

#include <functional>
#include <memory>
#include <type_traits>

#include "katana/Logging.h"

namespace katana {

/// A PreparedUpdate holds onto state that was created to update *something*
/// and waits to apply it until the owner of this object calls `Apply` if
/// `Apply` is not called, state is discarded. This is useful for preparing
/// several updates, checking for errors along the way, then applying all
/// of the updates atomically once certain they will all succeed.
///
/// In practice the user provides a callable object that will be invoked
/// later; The object may optionally take a boolean argument allowing for
/// clean up, the argument will be true if the object was invoked via
/// `Apply` and false if `Apply` was never called.
///
/// Example:
///   std::unique_ptr<PreparedUpdate> Thing::PrepareUpdateToThing() {
///     ...
///     auto state = std::make_unique<BigUpdateState>(...);
///     return PreparedUpdate::Make([state = std::move(state),
///                                  to_update = this](bool success) mutable {
///       if (success) {
///         to_update->ApplyBigUpdate(std::move(state));
///       } else {
///         to_update->Rollback();
///       }
///     });
///   }
///
///
///   Result<void> DoManyThingsToThing(Thing thing) {
///     auto update = thing.PrepareUpdateToThing();
///     ...
///     update->Apply();
///   }
class PreparedUpdate {
public:
  PreparedUpdate(const PreparedUpdate& no_copy) = delete;
  PreparedUpdate& operator=(const PreparedUpdate& no_copy) = delete;
  PreparedUpdate(PreparedUpdate&& no_move) = delete;
  PreparedUpdate& operator=(PreparedUpdate&& no_move) = delete;

  ~PreparedUpdate() {
    if (update_func_) {
      std::invoke(*update_func_, false);
      update_func_.reset();
    }
  }

  void Apply() {
    if (update_func_) {
      std::invoke(*update_func_, true);
      update_func_.reset();
    }
  }

  /// Build a PreparedUpdate from something callable. `update_func` can take
  /// zero or one argument. If it takes zero arguments it will be invoked only
  /// when Apply is called, else it will be either be invoked with `true` when
  /// apply is called, or `false` if the update is destroyed before Apply is
  /// called
  template <typename CallableUpdate>
  static std::unique_ptr<PreparedUpdate> Make(CallableUpdate&& update_func) {
    std::function<void(bool)> wrapper;
    if constexpr (std::is_invocable<CallableUpdate, bool>::value) {
      wrapper = MakeFunction(std::forward<CallableUpdate>(update_func));
    } else {
      wrapper = MakeFunction([f = std::forward<CallableUpdate>(update_func)](
                                 bool was_applied) mutable {
        if (was_applied) {
          std::invoke(f);
        }
      });
    }
    return std::unique_ptr<PreparedUpdate>(
        new PreparedUpdate(std::move(wrapper)));
  }

private:
  PreparedUpdate(std::function<void(bool)>&& update_func)
      : update_func_(std::make_unique<std::function<void(bool)>>(
            std::forward<decltype(update_func)>(update_func))) {}

  template <typename CallableUpdate>
  static std::function<void(bool)> MakeFunction(CallableUpdate&& update_func) {
    auto ptr = std::make_shared<CallableUpdate>(
        std::forward<CallableUpdate>(update_func));
    return [ptr](bool was_applied) { std::invoke(*ptr, was_applied); };
  }

  std::unique_ptr<std::function<void(bool)>> update_func_;
};

}  // namespace katana

#endif

#include "katana/WriteGroup.h"

#include "GlobalState.h"
#include "katana/Random.h"
#include "katana/Result.h"

template <typename T>
using Result = katana::Result<T>;

namespace {

constexpr uint32_t kTagLen = 12;

}  // namespace

Result<std::unique_ptr<katana::WriteGroup>>
katana::WriteGroup::Make() {
  // Don't use `OneHostOnly` because we can skip its broadcast
  std::string tag;
  if (Comm()->Rank == 0) {
    tag = katana::RandomAlphanumericString(kTagLen);
  }
  tag = Comm()->Broadcast(0, tag, kTagLen);
  return std::unique_ptr<WriteGroup>(new WriteGroup(tag));
}

Result<void>
katana::WriteGroup::Finish() {
  return async_op_group_.Finish();
}

void
katana::WriteGroup::AddOp(
    std::future<katana::CopyableResult<void>> future, const URI& file,
    uint64_t accounted_size) {
  if (accounted_size > kMaxOutstandingSize) {
    accounted_size = kMaxOutstandingSize;
  }
  if (accounted_size > 0) {
    while (outstanding_size_ + accounted_size > kMaxOutstandingSize) {
      if (!async_op_group_.FinishOne()) {
        KATANA_LOG_ERROR(
            "outstanding_size should be zero if we couldn't drain");
        break;
      }
    }
  }
  async_op_group_.AddOp(
      std::move(future), file,
      [wg = this, accounted_size]() -> katana::CopyableResult<void> {
        wg->outstanding_size_ -= accounted_size;
        return katana::CopyableResultSuccess();
      });
}

// shared pointer because FileFrames are often held that way due do the way
// they're used with arrow
void
katana::WriteGroup::StartStore(std::shared_ptr<katana::FileFrame> ff) {
  auto file = ff->path();
  uint64_t size = ff->map_size();

  // wrap future to hold onto FileFrame, but free it as soon as possible
  auto future = std::async(std::launch::async, [ff = std::move(ff)]() mutable {
    return ff->PersistAsync().get();
  });
  AddOp(std::move(future), file, size);
}

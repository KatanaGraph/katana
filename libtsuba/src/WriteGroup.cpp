#include "tsuba/WriteGroup.h"

#include "GlobalState.h"
#include "galois/Random.h"

template <typename T>
using Result = galois::Result<T>;

namespace {

constexpr uint32_t kTagLen = 12;

}  // namespace

namespace tsuba {

Result<std::unique_ptr<WriteGroup>>
WriteGroup::Make() {
  // Don't use `OneHostOnly` because we can skip its broadcast
  std::string tag;
  if (Comm()->ID == 0) {
    tag = galois::RandomAlphanumericString(kTagLen);
  }
  tag = Comm()->Broadcast(0, tag, kTagLen);
  return std::unique_ptr<WriteGroup>(new WriteGroup(tag));
}

Result<void>
WriteGroup::Finish() {
  Result<void> return_val = galois::ResultSuccess();
  uint32_t errors = 0;

  for (AsyncOp& op : pending_ops_) {
    auto res = op.result.get();
    if (!res) {
      GALOIS_LOG_DEBUG(
          "async write op for {} returned {}", op.location, res.error());
      errors++;
      return_val = res.error();
    }
  }

  if (errors > 0) {
    GALOIS_LOG_ERROR(
        "{} of {} async write ops returned errors", errors,
        pending_ops_.size());
  }

  return return_val;
}

void
WriteGroup::AddOp(std::future<galois::Result<void>> future, std::string file) {
  pending_ops_.emplace_back(AsyncOp{
      .result = std::move(future),
      .location = std::move(file),
  });
}

// shared pointer because FileFrames are often held that way due do the way
// they're used with arrow
void
WriteGroup::StartStore(std::shared_ptr<FileFrame> ff) {
  std::string file = ff->path();

  // wrap future to hold onto FileFrame, but free it as soon as possible
  auto future = std::async(std::launch::async, [ff = std::move(ff)]() mutable {
    return ff->PersistAsync().get();
  });
  AddOp(std::move(future), file);
}

}  // namespace tsuba

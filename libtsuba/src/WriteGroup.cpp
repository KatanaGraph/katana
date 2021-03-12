#include "tsuba/WriteGroup.h"

#include "GlobalState.h"
#include "katana/Random.h"

template <typename T>
using Result = katana::Result<T>;

namespace {

constexpr uint32_t kTagLen = 12;

}  // namespace

namespace tsuba {

Result<std::unique_ptr<WriteGroup>>
WriteGroup::Make() {
  // Don't use `OneHostOnly` because we can skip its broadcast
  std::string tag;
  if (Comm()->ID == 0) {
    tag = katana::RandomAlphanumericString(kTagLen);
  }
  tag = Comm()->Broadcast(0, tag, kTagLen);
  return std::unique_ptr<WriteGroup>(new WriteGroup(tag));
}

bool
WriteGroup::Drain() {
  auto op_it = pending_ops_.begin();
  if (op_it == pending_ops_.end()) {
    return false;
  }
  auto res = op_it->result.get();
  if (!res) {
    KATANA_LOG_DEBUG(
        "async write op for {} returned {}", op_it->location, res.error());
    errors_++;
    last_error_ = res.error();
  }
  outstanding_size_ -= op_it->accounted_size;
  pending_ops_.erase(op_it);
  return true;
}

Result<void>
WriteGroup::Finish() {
  while (Drain()) {
    // spin
  }

  if (errors_ > 0) {
    return last_error_.error().WithContext(
        "{} of {} async write ops returned errors", errors_, total_);
  }

  return last_error_;
}

void
WriteGroup::AddOp(
    std::future<katana::Result<void>> future, std::string file,
    uint64_t accounted_size) {
  if (accounted_size > kMaxOutstandingSize) {
    accounted_size = kMaxOutstandingSize;
  }
  if (accounted_size > 0) {
    while (outstanding_size_ + accounted_size > kMaxOutstandingSize) {
      if (!Drain()) {
        KATANA_LOG_ERROR(
            "outstanding_size should be zero if we couldn't drain");
        break;
      }
    }
  }
  pending_ops_.emplace_back(AsyncOp{
      .result = std::move(future),
      .location = std::move(file),
      .accounted_size = accounted_size,
  });
  total_ += 1;
}

// shared pointer because FileFrames are often held that way due do the way
// they're used with arrow
void
WriteGroup::StartStore(std::shared_ptr<FileFrame> ff) {
  std::string file = ff->path();
  uint64_t size = ff->map_size();

  // wrap future to hold onto FileFrame, but free it as soon as possible
  auto future = std::async(std::launch::async, [ff = std::move(ff)]() mutable {
    return ff->PersistAsync().get();
  });
  AddOp(std::move(future), file, size);
}

}  // namespace tsuba

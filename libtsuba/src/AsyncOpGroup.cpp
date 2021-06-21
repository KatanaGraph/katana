#include "tsuba/AsyncOpGroup.h"

bool
tsuba::AsyncOpGroup::FinishOne() {
  auto op_it = pending_ops_.begin();
  if (op_it == pending_ops_.end()) {
    return false;
  }
  auto res = op_it->result.get();
  if (!res) {
    KATANA_LOG_ERROR(
        "async op for {} returned {}", op_it->location, res.error());
    errors_++;
    last_error_ = res.error();
  } else {
    res = op_it->on_complete();
    if (!res) {
      KATANA_LOG_ERROR(
          "complete cb for async op for {} returned {}", op_it->location,
          res.error());
    }
  }
  pending_ops_.erase(op_it);
  return true;
}

katana::Result<void>
tsuba::AsyncOpGroup::Finish() {
  while (FinishOne()) {
    // Wait for all ops
  }

  if (errors_ > 0) {
    return last_error_.WithContext(
        "{} of {} async write ops returned errors", errors_, total_);
  }

  return katana::ResultSuccess();
}

void
tsuba::AsyncOpGroup::AddOp(
    std::future<katana::CopyableResult<void>> future, std::string file,
    const std::function<katana::CopyableResult<void>()>& on_complete) {
  pending_ops_.emplace_back(AsyncOp{
      .result = std::move(future),
      .location = std::move(file),
      .on_complete = on_complete,
  });
  total_ += 1;
}

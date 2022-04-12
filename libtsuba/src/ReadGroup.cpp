#include "katana/ReadGroup.h"

void
katana::ReadGroup::AddOp(
    std::future<katana::CopyableResult<void>> future, const URI& file,
    const std::function<katana::CopyableResult<void>()>& on_complete) {
  async_op_group_.AddOp(std::move(future), file, on_complete);
}

katana::Result<void>
katana::ReadGroup::Finish() {
  return async_op_group_.Finish();
}

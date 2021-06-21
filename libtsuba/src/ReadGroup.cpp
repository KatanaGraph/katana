#include "tsuba/ReadGroup.h"

void
tsuba::ReadGroup::AddOp(
    std::future<katana::CopyableResult<void>> future, std::string file,
    const std::function<katana::CopyableResult<void>()>& on_complete) {
  async_op_group_.AddOp(std::move(future), std::move(file), on_complete);
}

katana::Result<void>
tsuba::ReadGroup::Finish() {
  return async_op_group_.Finish();
}

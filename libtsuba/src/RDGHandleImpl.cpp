#include "RDGHandleImpl.h"

#include "tsuba/Errors.h"

template <typename T>
using Result = katana::Result<T>;

namespace tsuba {

Result<void>
RDGHandleImpl::Validate() const {
  if (rdg_manifest_.dir().empty()) {
    KATANA_LOG_DEBUG("rdg_manifest_.dir() is empty");
    return ErrorCode::InvalidArgument;
  }
  return katana::ResultSuccess();
}

}  // namespace tsuba

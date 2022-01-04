#include "RDGHandleImpl.h"

#include "katana/ErrorCode.h"

katana::Result<void>
katana::RDGHandleImpl::Validate() const {
  if (rdg_manifest_.dir().empty()) {
    KATANA_LOG_DEBUG("rdg_manifest_.dir() is empty");
    return ErrorCode::InvalidArgument;
  }
  return katana::ResultSuccess();
}

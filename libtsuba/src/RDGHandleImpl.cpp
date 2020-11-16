#include "RDGHandleImpl.h"

#include "tsuba/Errors.h"

template <typename T>
using Result = galois::Result<T>;

namespace tsuba {

Result<void>
RDGHandleImpl::Validate() const {
  if (rdg_meta.dir().empty()) {
    GALOIS_LOG_DEBUG("rdg_meta.dir(): \"{}\" is empty", rdg_meta.dir());
    return ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

}  // namespace tsuba

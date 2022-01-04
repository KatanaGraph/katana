#include "katana/ErrorCode.h"

#include "katana/Logging.h"

katana::internal::ErrorCodeCategory::~ErrorCodeCategory() = default;

const katana::internal::ErrorCodeCategory&
katana::internal::GetErrorCodeCategory() {
  static ErrorCodeCategory c;
  return c;
}

katana::ErrorCode
katana::ArrowToKatana(arrow::StatusCode code) {
  KATANA_LOG_DEBUG_ASSERT(code != arrow::StatusCode::OK);

  switch (code) {
  case arrow::StatusCode::Invalid:
    return katana::ErrorCode::InvalidArgument;
  case arrow::StatusCode::OutOfMemory:
    return katana::ErrorCode::OutOfMemory;
  default:
    return katana::ErrorCode::ArrowError;
  }
}

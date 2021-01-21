#include "tsuba/Errors.h"

#include "katana/Logging.h"

tsuba::internal::ErrorCodeCategory::~ErrorCodeCategory() = default;

const tsuba::internal::ErrorCodeCategory&
tsuba::internal::GetErrorCodeCategory() {
  static ErrorCodeCategory c;
  return c;
}

tsuba::ErrorCode
tsuba::ArrowToTsuba(arrow::StatusCode code) {
  KATANA_LOG_DEBUG_ASSERT(code != arrow::StatusCode::OK);

  switch (code) {
  case arrow::StatusCode::Invalid:
    return tsuba::ErrorCode::InvalidArgument;
  case arrow::StatusCode::OutOfMemory:
    return tsuba::ErrorCode::OutOfMemory;
  default:
    return tsuba::ErrorCode::ArrowError;
  }
}

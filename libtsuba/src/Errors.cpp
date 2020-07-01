#include "tsuba/Errors.h"

const tsuba::internal::ErrorCodeCategory&
tsuba::internal::GetErrorCodeCategory() {
  static ErrorCodeCategory c;
  return c;
}

tsuba::ErrorCode tsuba::ArrowToTsuba(arrow::StatusCode code) {
  switch (code) {
  case arrow::StatusCode::OK:
    return tsuba::ErrorCode::Success;
  case arrow::StatusCode::Invalid:
    return tsuba::ErrorCode::InvalidArgument;
  case arrow::StatusCode::OutOfMemory:
    return tsuba::ErrorCode::OutOfMemory;
  default:
    return tsuba::ErrorCode::ArrowError;
  }
}

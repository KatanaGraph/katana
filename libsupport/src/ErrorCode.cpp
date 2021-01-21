#include "katana/ErrorCode.h"

katana::internal::ErrorCodeCategory::~ErrorCodeCategory() = default;

const katana::internal::ErrorCodeCategory&
katana::internal::GetErrorCodeCategory() {
  static ErrorCodeCategory c;
  return c;
}

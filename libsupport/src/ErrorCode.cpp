#include "katana/ErrorCode.h"

const katana::internal::ErrorCodeCategory&
katana::internal::GetErrorCodeCategory() {
  static ErrorCodeCategory c;
  return c;
}

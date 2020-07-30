#include "galois/ErrorCode.h"

const galois::internal::ErrorCodeCategory&
galois::internal::GetErrorCodeCategory() {
  static ErrorCodeCategory c;
  return c;
}

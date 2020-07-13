#include "galois/ErrorCode.h"

const support::internal::ErrorCodeCategory&
support::internal::GetErrorCodeCategory() {
  static ErrorCodeCategory c;
  return c;
}

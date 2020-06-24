#include "tsuba/Errors.h"

const tsuba::internal::ErrorCodeCategory&
tsuba::internal::GetErrorCodeCategory() {
  static ErrorCodeCategory c;
  return c;
}

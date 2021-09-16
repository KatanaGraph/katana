#include <type_traits>

#include <arrow/api.h>
#include <arrow/type_traits.h>

#include "katana/ArrowVisitor.h"

namespace {

template <typename T>
std::enable_if_t<arrow::is_string_like_type<T>::value, int>
IsStringLikeTypePatchedNeeded(void*) {
  return false;
}

template <typename T>
std::enable_if_t<!arrow::is_string_like_type<T>::value, int>
IsStringLikeTypePatchedNeeded(void*) {
  return false;
}

template <typename>
int
IsStringLikeTypePatchedNeeded(...) {
  // If this overload is selected, arrow::is_string_like_type<T> is neither
  // true nor false.
  return true;
}

void
TestIsStringLikeTypePatchedNeeded() {
  KATANA_LOG_ASSERT(IsStringLikeTypePatchedNeeded<arrow::BooleanType>(nullptr));
}

}  // namespace

int
main() {
  TestIsStringLikeTypePatchedNeeded();
}

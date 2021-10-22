#include <arrow/type_fwd.h>

#include "katana/ArrowVisitor.h"
#include "katana/Result.h"

namespace {

constexpr int NUM_ENTRIES = 10;

katana::Result<void>
TestNulls() {
  std::vector<std::shared_ptr<arrow::Scalar>> scalars(NUM_ENTRIES);
  std::shared_ptr<arrow::Array> array =
      KATANA_CHECKED(katana::ArrayFromScalars(scalars, arrow::null()));
  // NB: length() is signed, size() is not - but they should both have small
  // enough values that it doesn't matter
  KATANA_LOG_VASSERT(
      static_cast<size_t>(array->length()) == scalars.size(),
      "array length: {}, vector size: {}", array->length(), scalars.size());
  KATANA_LOG_ASSERT(array->length() == array->null_count());
  KATANA_LOG_ASSERT(array->null_count() == NUM_ENTRIES);
  return katana::ResultSuccess();
}

katana::Result<void>
TestAll() {
  KATANA_CHECKED(TestNulls());
  return katana::ResultSuccess();
}

}  // namespace

int
main() {
  KATANA_LOG_ASSERT(TestAll());
  return 0;
}

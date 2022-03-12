#include "katana/HashIndexedProperty.h"
#include <arrow/compute/api.h>

katana::Result<katana::HashIndexedProperty>
katana::HashIndexedProperty::Deflate(const arrow::Array& array) {
  HashMap index;
  int64_t next_pos = 0;
  for (int64_t i = 0, n = array.length(); i < n; ++i) {
    if (array.IsValid(i)) {
      index[i] = next_pos++;
    }
  }

  std::shared_ptr<arrow::Array> data =
      KATANA_CHECKED(arrow::compute::DropNulls(array));

  return HashIndexedProperty(std::move(index), std::move(data));
}

katana::Result<std::shared_ptr<arrow::Array>>
katana::HashIndexedProperty::Inflate() const {

}

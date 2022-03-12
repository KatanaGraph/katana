#include "katana/HashIndexedProperty.h"

#include <arrow/compute/api.h>

#include "katana/ArrowRandomAccessBuilder.h"

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
      KATANA_CHECKED(arrow::compute::DropNull(array));

  return HashIndexedProperty(std::move(index), std::move(data), array.length());
}

katana::Result<std::shared_ptr<arrow::Array>>
katana::HashIndexedProperty::Inflate() const {
  std::shared_ptr<arrow::Array> dense_map = KATANA_CHECKED(MakeDenseMap());
  return KATANA_CHECKED(arrow::compute::Take(*dense_map, *data_));
}

katana::Result<std::shared_ptr<arrow::Array>>
katana::HashIndexedProperty::MakeDenseMap() const {
  katana::ArrowRandomAccessBuilder<arrow::Int64Type> builder(length_);
  for (auto [k, v] : index_) {
    builder[k] = v;
  }
  return builder.Finalize();
}

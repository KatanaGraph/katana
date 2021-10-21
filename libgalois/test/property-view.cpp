#include "katana/Properties.h"
#include "katana/Result.h"

namespace {
constexpr int NUM_ARRAY_ENTRIES = 10;

katana::Result<std::shared_ptr<arrow::Int8Array>>
AllValid() {
  arrow::NumericBuilder<arrow::Int8Type> builder;
  KATANA_LOG_ASSERT(builder.AppendEmptyValues(NUM_ARRAY_ENTRIES).ok());
  std::shared_ptr<arrow::Int8Array> array;
  KATANA_LOG_ASSERT(builder.Finish(&array).ok());

  return array;
}

katana::Result<std::shared_ptr<arrow::Int8Array>>
NoValid() {
  arrow::NumericBuilder<arrow::Int8Type> builder;
  KATANA_LOG_ASSERT(builder.AppendNulls(NUM_ARRAY_ENTRIES).ok());
  std::shared_ptr<arrow::Int8Array> tmp_array;
  KATANA_LOG_ASSERT(builder.Finish(&tmp_array).ok());

  std::shared_ptr<arrow::Buffer> no_bitmap;

  std::shared_ptr<arrow::ArrayData> data = tmp_array->data()->Copy();
  data->buffers[0] = no_bitmap;

  std::shared_ptr<arrow::Array> array = arrow::MakeArray(data);
  return std::static_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(array);
}
}  // namespace

katana::Result<void>
TestNoBitmapValidity() {
  auto valid_array = KATANA_CHECKED(AllValid());
  KATANA_LOG_ASSERT(valid_array->length() == NUM_ARRAY_ENTRIES);
  KATANA_LOG_ASSERT(valid_array->null_count() == 0);
  KATANA_LOG_ASSERT(valid_array->null_bitmap_data() == nullptr);

  auto valid_view = KATANA_CHECKED(
      katana::PODPropertyView<int8_t>::Make<arrow::Int8Type>(*valid_array));
  for (size_t i = 0; i < NUM_ARRAY_ENTRIES; ++i) {
    KATANA_LOG_ASSERT(valid_view.IsValid(i));
  }

  auto null_array = KATANA_CHECKED(NoValid());
  KATANA_LOG_ASSERT(null_array->length() == NUM_ARRAY_ENTRIES);
  KATANA_LOG_ASSERT(null_array->null_count() == NUM_ARRAY_ENTRIES);
  KATANA_LOG_ASSERT(null_array->null_bitmap_data() == nullptr);

  auto null_view = KATANA_CHECKED(
      katana::PODPropertyView<int8_t>::Make<arrow::Int8Type>(*null_array));
  for (size_t i = 0; i < NUM_ARRAY_ENTRIES; ++i) {
    KATANA_LOG_ASSERT(!null_view.IsValid(i));
  }

  return katana::ResultSuccess();
}

katana::Result<void>
TestAll() {
  return TestNoBitmapValidity();
}

int
main(int, char**) {
  KATANA_LOG_ASSERT(TestAll());
  return 0;
}

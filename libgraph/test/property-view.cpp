#include "katana/Properties.h"
#include "katana/Result.h"

namespace {

constexpr int kNumRows = 10;
constexpr int kNumArrayEntries = 10;

katana::Result<std::shared_ptr<arrow::Int8Array>>
AllValid() {
  arrow::NumericBuilder<arrow::Int8Type> builder;
  KATANA_LOG_ASSERT(builder.AppendEmptyValues(kNumArrayEntries).ok());
  std::shared_ptr<arrow::Int8Array> array;
  KATANA_LOG_ASSERT(builder.Finish(&array).ok());

  return array;
}

katana::Result<std::shared_ptr<arrow::Int8Array>>
NoValid() {
  arrow::NumericBuilder<arrow::Int8Type> builder;
  KATANA_LOG_ASSERT(builder.AppendNulls(kNumArrayEntries).ok());
  std::shared_ptr<arrow::Int8Array> tmp_array;
  KATANA_LOG_ASSERT(builder.Finish(&tmp_array).ok());

  std::shared_ptr<arrow::Buffer> no_bitmap;

  std::shared_ptr<arrow::ArrayData> data = tmp_array->data()->Copy();
  data->buffers[0] = no_bitmap;

  std::shared_ptr<arrow::Array> array = arrow::MakeArray(data);
  return std::static_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(array);
}

/// Generates a fixed sized binary array for checking with a view.
katana::Result<std::shared_ptr<arrow::FixedSizeBinaryArray>>
GenerateFixedBinaryTestArray() {
  constexpr size_t binary_size = sizeof(int) * kNumArrayEntries;
  auto fixed_size_type = KATANA_CHECKED_CONTEXT(
      arrow::FixedSizeBinaryType::Make(binary_size),
      "failed to make fixed size type of size {}", binary_size);
  arrow::FixedSizeBinaryBuilder fixed_sized_binary_builder(fixed_size_type);

  for (size_t row = 0; row < kNumRows; row++) {
    std::array<int, kNumArrayEntries> to_write;
    for (size_t index = 0; index < kNumArrayEntries; index++) {
      to_write.at(index) = row + index;
    }
    KATANA_CHECKED(fixed_sized_binary_builder.Append(
        reinterpret_cast<uint8_t*>(to_write.data())));
  }

  std::shared_ptr<arrow::Array> array_of_fixed_size_binaries =
      KATANA_CHECKED_CONTEXT(
          fixed_sized_binary_builder.Finish(),
          "failed to finish fixed size binary builder");

  return katana::Result<std::shared_ptr<arrow::FixedSizeBinaryArray>>(
      std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(
          array_of_fixed_size_binaries));
}

}  // namespace

katana::Result<void>
TestNoBitmapValidity() {
  auto valid_array = KATANA_CHECKED(AllValid());
  KATANA_LOG_ASSERT(valid_array->length() == kNumArrayEntries);
  KATANA_LOG_ASSERT(valid_array->null_count() == 0);
  KATANA_LOG_ASSERT(valid_array->null_bitmap_data() == nullptr);

  auto valid_view = KATANA_CHECKED(
      katana::PODPropertyView<int8_t>::Make<arrow::Int8Type>(*valid_array));
  for (size_t i = 0; i < kNumArrayEntries; ++i) {
    KATANA_LOG_ASSERT(valid_view.IsValid(i));
  }

  auto null_array = KATANA_CHECKED(NoValid());
  KATANA_LOG_ASSERT(null_array->length() == kNumArrayEntries);
  KATANA_LOG_ASSERT(null_array->null_count() == kNumArrayEntries);
  KATANA_LOG_ASSERT(null_array->null_bitmap_data() == nullptr);

  auto null_view = KATANA_CHECKED(
      katana::PODPropertyView<int8_t>::Make<arrow::Int8Type>(*null_array));
  for (size_t i = 0; i < kNumArrayEntries; ++i) {
    KATANA_LOG_ASSERT(!null_view.IsValid(i));
  }

  return katana::ResultSuccess();
}

/// Simple test to make sure the view is sane for a simple FixedSizeBinary.
katana::Result<void>
TestFixedSizedBinaryArray() {
  std::shared_ptr<arrow::FixedSizeBinaryArray> test_array =
      KATANA_CHECKED(GenerateFixedBinaryTestArray());
  auto view = KATANA_CHECKED(
      (katana::FixedSizeBinaryPODArrayView<int, kNumArrayEntries>::Make(
          *test_array)));

  for (size_t row = 0; row < kNumRows; row++) {
    for (size_t index = 0; index < kNumArrayEntries; index++) {
      size_t expected = row + index;
      KATANA_LOG_VASSERT(
          static_cast<size_t>(view[row][index]) == expected,
          "expected {} for row {} index {} but found  {}", expected, row, index,
          view[row][index]);
    }
  }

  return katana::ResultSuccess();
}

katana::Result<void>
TestAll() {
  KATANA_CHECKED(TestNoBitmapValidity());
  KATANA_CHECKED(TestFixedSizedBinaryArray());
  return katana::ResultSuccess();
}

int
main() {
  auto res = TestAll();
  if (!res) {
    KATANA_LOG_FATAL("a test failed to run: {}", res.error());
  }
  return 0;
}

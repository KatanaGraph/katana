#include "tsuba/ParquetWriter.h"

#include <arrow/chunked_array.h>

#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"

template <typename T>
using Result = katana::Result<T>;

namespace {

// constant taken directly from the arrow docs
constexpr uint64_t kMaxStringChunkSize = 0x7FFFFFFE;

std::shared_ptr<parquet::WriterProperties>
StandardWriterProperties() {
  // int64 timestamps with nanosecond resolution requires Parquet version 2.0.
  // In Arrow to Parquet version 1.0, nanosecond timestamps will get truncated
  // to milliseconds.
  return parquet::WriterProperties::Builder()
      .version(parquet::ParquetVersion::PARQUET_2_0)
      ->data_page_version(parquet::ParquetDataPageVersion::V2)
      ->build();
}

std::shared_ptr<parquet::ArrowWriterProperties>
StandardArrowProperties() {
  return parquet::ArrowWriterProperties::Builder().build();
}

/// Store the arrow table in a file
katana::Result<void>
StoreParquet(
    const arrow::Table& table, const katana::Uri& uri,
    tsuba::WriteGroup* desc) {
  auto ff = std::make_shared<tsuba::FileFrame>();
  if (auto res = ff->Init(); !res) {
    return res.error().WithContext("creating output buffer");
  }

  auto write_result = parquet::arrow::WriteTable(
      table, arrow::default_memory_pool(), ff,
      std::numeric_limits<int64_t>::max(), StandardWriterProperties(),
      StandardArrowProperties());

  if (!write_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}", write_result);
  }

  ff->Bind(uri.string());
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
  if (!desc) {
    return ff->Persist();
  }

  desc->StartStore(std::move(ff));
  return katana::ResultSuccess();
}

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
LargeStringToChunkedString(
    const std::shared_ptr<arrow::LargeStringArray>& arr) {
  std::vector<std::shared_ptr<arrow::Array>> chunks;

  arrow::StringBuilder builder;

  uint64_t inserted = 0;

  for (uint64_t i = 0, size = arr->length(); i < size; ++i) {
    if (!arr->IsValid(i)) {
      auto status = builder.AppendNull();
      if (!status.ok()) {
        return KATANA_ERROR(
            tsuba::ErrorCode::ArrowError, "appending null: {}", status);
      }
      continue;
    }
    arrow::util::string_view val = arr->GetView(i);
    uint64_t val_size = val.size();
    KATANA_LOG_ASSERT(val_size < kMaxStringChunkSize);
    if (inserted + val_size >= kMaxStringChunkSize) {
      std::shared_ptr<arrow::Array> new_arr;
      auto status = builder.Finish(&new_arr);
      if (!status.ok()) {
        return KATANA_ERROR(
            tsuba::ErrorCode::ArrowError, "finishing string array: {}", status);
      }
      chunks.emplace_back(new_arr);
      inserted = 0;
      builder.Reset();
    }
    inserted += val_size;
    auto status = builder.Append(val);
    if (!status.ok()) {
      return KATANA_ERROR(
          tsuba::ErrorCode::ArrowError, "adding string to array: {}", status);
    }
  }
  if (inserted > 0) {
    std::shared_ptr<arrow::Array> new_arr;
    auto status = builder.Finish(&new_arr);
    if (!status.ok()) {
      return KATANA_ERROR(
          tsuba::ErrorCode::ArrowError, "finishing string array: {}", status);
    }
    chunks.emplace_back(new_arr);
  }

  auto maybe_res = arrow::ChunkedArray::Make(chunks, arrow::utf8());
  if (!maybe_res.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "building chunked array: {}",
        maybe_res.status());
  }
  return maybe_res.ValueOrDie();
}

// HandleBadParquetTypes here and HandleBadParquetTypes in ParquetReader.cpp
// workaround a libarrow2.0 limitation in reading and writing LargeStrings to
// parquet files.
katana::Result<std::shared_ptr<arrow::ChunkedArray>>
HandleBadParquetTypes(std::shared_ptr<arrow::ChunkedArray> old_array) {
  if (old_array->num_chunks() > 1) {
    return old_array;
  }
  switch (old_array->type()->id()) {
  case arrow::Type::type::LARGE_STRING: {
    std::shared_ptr<arrow::LargeStringArray> large_string_array =
        std::static_pointer_cast<arrow::LargeStringArray>(old_array->chunk(0));
    return LargeStringToChunkedString(large_string_array);
  }
  default:
    return old_array;
  }
}

}  // namespace

Result<std::unique_ptr<tsuba::ParquetWriter>>
tsuba::ParquetWriter::Make(
    const std::shared_ptr<arrow::ChunkedArray>& array,
    const std::string& name) {
  auto res = HandleBadParquetTypes(array);
  if (!res) {
    return res.error().WithContext("handling arrow->parquet type mismatch");
  }
  std::shared_ptr<arrow::ChunkedArray> new_array(std::move(res.value()));

  std::shared_ptr<arrow::Table> column = arrow::Table::Make(
      arrow::schema({arrow::field(name, new_array->type())}), {new_array});
  return std::unique_ptr<ParquetWriter>(new ParquetWriter(column));
}

katana::Result<void>
tsuba::ParquetWriter::WriteToUri(const katana::Uri& uri, WriteGroup* group) {
  try {
    return StoreParquet(*table_, uri, group);
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

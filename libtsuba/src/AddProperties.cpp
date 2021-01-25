#include "AddProperties.h"

#include <arrow/chunked_array.h>

#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FileView.h"

namespace {

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
ChunkedStringToLargeString(const std::shared_ptr<arrow::ChunkedArray>& arr) {
  arrow::LargeStringBuilder builder;

  for (const auto& chunk : arr->chunks()) {
    std::shared_ptr<arrow::StringArray> string_array =
        std::static_pointer_cast<arrow::StringArray>(chunk);
    for (uint64_t i = 0, size = string_array->length(); i < size; ++i) {
      if (!string_array->IsValid(i)) {
        auto status = builder.AppendNull();
        if (!status.ok()) {
          KATANA_LOG_ERROR("could not append null: {}", status);
          return tsuba::ErrorCode::ArrowError;
        }
        continue;
      }
      auto status = builder.Append(string_array->GetView(i));
      if (!status.ok()) {
        KATANA_LOG_ERROR("could not add string to array builder: {}", status);
        return tsuba::ErrorCode::ArrowError;
      }
    }
  }

  std::shared_ptr<arrow::Array> new_arr;
  auto status = builder.Finish(&new_arr);
  if (!status.ok()) {
    KATANA_LOG_ERROR("could not finish building string array: {}", status);
    return tsuba::ErrorCode::ArrowError;
  }

  auto maybe_res = arrow::ChunkedArray::Make({new_arr}, arrow::large_utf8());
  if (!maybe_res.ok()) {
    KATANA_LOG_ERROR(
        "could not make arrow chunked array: {}", maybe_res.status());
    return tsuba::ErrorCode::ArrowError;
  }
  return maybe_res.ValueOrDie();
}

// HandleBadParquetTypes here and HandleBadParquetTypes in RDG.cpp
// workaround a libarrow2.0 limitation in reading and writing LargeStrings to
// parquet files.
katana::Result<std::shared_ptr<arrow::ChunkedArray>>
HandleBadParquetTypes(std::shared_ptr<arrow::ChunkedArray> old_array) {
  if (old_array->num_chunks() <= 1) {
    return old_array;
  }
  switch (old_array->type()->id()) {
  case arrow::Type::type::STRING: {
    return ChunkedStringToLargeString(old_array);
  }
  default:
    return old_array;
  }
}

katana::Result<std::shared_ptr<arrow::Table>>
DoLoadProperties(
    const std::string& expected_name, const katana::Uri& file_path) {
  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  if (auto res = fv->Bind(file_path.string(), false); !res) {
    KATANA_LOG_DEBUG("bind error: {}", res.error());
    return res.error();
  }

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  if (!open_file_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}", open_file_result);
  }

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadTable(&out);
  if (!read_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}", read_result);
  }

  auto fixed_column_res = HandleBadParquetTypes(out->column(0));
  if (!fixed_column_res) {
    return fixed_column_res.error();
  }
  std::shared_ptr<arrow::ChunkedArray> fixed_column(
      std::move(fixed_column_res.value()));

  out = arrow::Table::Make(
      arrow::schema(
          {arrow::field(out->field(0)->name(), fixed_column->type())}),
      {fixed_column});

  // Combine multiple chunks into one. Binary and string columns (c.f. large
  // binary and large string columns) are a special case. They may not be
  // combined into a single chunk due to the fact the offset type for these
  // columns is int32_t and thus the maximum size of an arrow::Array for these
  // types is 2^31.
  auto combine_result = out->CombineChunks(arrow::default_memory_pool());
  if (!combine_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}",
        combine_result.status());
  }

  out = std::move(combine_result.ValueOrDie());

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument, "expected 1 field found {} instead",
        schema->num_fields());
  }

  if (schema->field(0)->name() != expected_name) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument, "expected {} found {} instead",
        expected_name, schema->field(0)->name());
  }

  return out;
}

katana::Result<std::shared_ptr<arrow::Table>>
DoLoadPropertySlice(
    const std::string& expected_name, const katana::Uri& file_path,
    int64_t offset, int64_t length) {
  if (offset < 0 || length < 0) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  if (auto res = fv->Bind(file_path.string(), 0, 0, false); !res) {
    return res.error();
  }

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  if (!open_file_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}", open_file_result);
  }

  std::vector<int> row_groups;
  int rg_count = reader->num_row_groups();
  int64_t row_offset = 0;
  int64_t cumulative_rows = 0;
  int64_t file_offset = 0;
  int64_t cumulative_bytes = 0;
  for (int i = 0; cumulative_rows < offset + length && i < rg_count; ++i) {
    auto rg_md = reader->parquet_reader()->metadata()->RowGroup(i);
    int64_t new_rows = rg_md->num_rows();
    int64_t new_bytes = rg_md->total_byte_size();
    if (offset < cumulative_rows + new_rows) {
      if (row_groups.empty()) {
        row_offset = offset - cumulative_rows;
        file_offset = cumulative_bytes;
      }
      row_groups.push_back(i);
    }
    cumulative_rows += new_rows;
    cumulative_bytes += new_bytes;
  }

  if (auto res = fv->Fill(file_offset, cumulative_bytes, false); !res) {
    return res.error();
  }

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadRowGroups(row_groups, &out);
  if (!read_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}", read_result);
  }

  auto combine_result = out->CombineChunks(arrow::default_memory_pool());
  if (!combine_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}",
        combine_result.status());
  }

  out = std::move(combine_result.ValueOrDie());

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument, "expected 1 field found {} instead",
        schema->num_fields());
  }

  if (schema->field(0)->name() != expected_name) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument,
        "expected column {} found {} instead", expected_name,
        schema->field(0)->name());
  }

  return out->Slice(row_offset, length);
}

}  // namespace

katana::Result<std::shared_ptr<arrow::Table>>
tsuba::LoadProperties(
    const std::string& expected_name, const katana::Uri& file_path) {
  try {
    return DoLoadProperties(expected_name, file_path);
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

katana::Result<std::shared_ptr<arrow::Table>>
tsuba::LoadPropertySlice(
    const std::string& expected_name, const katana::Uri& file_path,
    int64_t offset, int64_t length) {
  try {
    return DoLoadPropertySlice(expected_name, file_path, offset, length);
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

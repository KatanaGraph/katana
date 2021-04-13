#include "tsuba/ParquetReader.h"

#include "tsuba/Errors.h"
#include "tsuba/FileView.h"

template <typename T>
using Result = katana::Result<T>;

namespace {

Result<std::shared_ptr<arrow::ChunkedArray>>
ChunkedStringToLargeString(const std::shared_ptr<arrow::ChunkedArray>& arr) {
  arrow::LargeStringBuilder builder;

  for (const auto& chunk : arr->chunks()) {
    std::shared_ptr<arrow::StringArray> string_array =
        std::static_pointer_cast<arrow::StringArray>(chunk);
    for (uint64_t i = 0, size = string_array->length(); i < size; ++i) {
      if (!string_array->IsValid(i)) {
        auto status = builder.AppendNull();
        if (!status.ok()) {
          return KATANA_ERROR(
              tsuba::ErrorCode::ArrowError, "appending null to array: {}",
              status);
        }
        continue;
      }
      auto status = builder.Append(string_array->GetView(i));
      if (!status.ok()) {
        return KATANA_ERROR(
            tsuba::ErrorCode::ArrowError, "appending string to array: {}",
            status);
      }
    }
  }

  std::shared_ptr<arrow::Array> new_arr;
  auto status = builder.Finish(&new_arr);
  if (!status.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "finishing array: {}", status);
  }

  auto maybe_res = arrow::ChunkedArray::Make({new_arr}, arrow::large_utf8());
  if (!maybe_res.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "building chunked array: {}",
        maybe_res.status());
  }
  return maybe_res.ValueOrDie();
}

// HandleBadParquetTypes here and HandleBadParquetTypes in ParquetWriter.cpp
// workaround a libarrow2.0 limitation in reading and writing LargeStrings to
// parquet files.
katana::Result<std::shared_ptr<arrow::ChunkedArray>>
HandleBadParquetTypes(std::shared_ptr<arrow::ChunkedArray> old_array) {
  switch (old_array->type()->id()) {
  case arrow::Type::type::STRING: {
    return ChunkedStringToLargeString(old_array);
  }
  default:
    return old_array;
  }
}

}  // namespace

Result<std::unique_ptr<tsuba::ParquetReader>>
tsuba::ParquetReader::Make(std::optional<Slice> slice) {
  return std::unique_ptr<ParquetReader>(new ParquetReader(slice));
}

// Internal use only, invoke iff slice_ has a value
Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadFromUriSliced(const katana::Uri& uri) {
  auto slice = slice_.value();
  if (slice.offset < 0 || slice.length < 0) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  if (auto res = fv->Bind(uri.string(), 0, 0, false); !res) {
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
  int64_t last_row = slice.offset + slice.length;
  for (int i = 0; cumulative_rows < last_row && i < rg_count; ++i) {
    auto rg_md = reader->parquet_reader()->metadata()->RowGroup(i);
    int64_t new_rows = rg_md->num_rows();
    int64_t new_bytes = rg_md->total_byte_size();
    if (slice.offset < cumulative_rows + new_rows) {
      if (row_groups.empty()) {
        row_offset = slice.offset - cumulative_rows;
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

  out = out->Slice(row_offset, slice.length);

  auto combine_result = out->CombineChunks(arrow::default_memory_pool());
  if (!combine_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}",
        combine_result.status());
  }
  return combine_result.ValueOrDie();
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadFromUri(const katana::Uri& uri) {
  if (slice_) {
    // logic for a sliced read is different enough not to bother trying
    // to DRY these out
    return ReadFromUriSliced(uri);
  }

  auto fv = std::make_shared<tsuba::FileView>();
  if (auto res = fv->Bind(uri.string(), false); !res) {
    return res.error().WithContext("preparing read buffer");
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

  std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;
  arrow::SchemaBuilder schema_builder;
  for (int i = 0, size = out->num_columns(); i < size; ++i) {
    auto fixed_column_res = HandleBadParquetTypes(out->column(i));
    if (!fixed_column_res) {
      return fixed_column_res.error();
    }
    std::shared_ptr<arrow::ChunkedArray> fixed_column(
        std::move(fixed_column_res.value()));
    new_columns.emplace_back(fixed_column);
    auto new_field = std::make_shared<arrow::Field>(
        out->field(i)->name(), fixed_column->type());
    if (auto status = schema_builder.AddField(new_field); !status.ok()) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "adding field to table schema: {}", status);
    }
  }
  auto maybe_schema = schema_builder.Finish();
  if (!maybe_schema.ok()) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "finishing table schema: {}",
        maybe_schema.status());
  }
  std::shared_ptr<arrow::Schema> final_schema = maybe_schema.ValueOrDie();

  out = arrow::Table::Make(final_schema, new_columns);

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

  return combine_result.ValueOrDie();
}

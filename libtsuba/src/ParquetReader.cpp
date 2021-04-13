#include "tsuba/ParquetReader.h"

#include <limits>
#include <memory>
#include <unordered_map>

#include <arrow/chunked_array.h>
#include <arrow/type.h>

#include "tsuba/Errors.h"
#include "tsuba/FileView.h"

template <typename T>
using Result = katana::Result<T>;

using ErrorCode = tsuba::ErrorCode;

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
              ErrorCode::ArrowError, "appending null to array: {}", status);
        }
        continue;
      }
      auto status = builder.Append(string_array->GetView(i));
      if (!status.ok()) {
        return KATANA_ERROR(
            ErrorCode::ArrowError, "appending string to array: {}", status);
      }
    }
  }

  std::shared_ptr<arrow::Array> new_arr;
  auto status = builder.Finish(&new_arr);
  if (!status.ok()) {
    return KATANA_ERROR(ErrorCode::ArrowError, "finishing array: {}", status);
  }

  auto maybe_res = arrow::ChunkedArray::Make({new_arr}, arrow::large_utf8());
  if (!maybe_res.ok()) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "building chunked array: {}",
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

Result<std::unique_ptr<parquet::arrow::FileReader>>
MakeFileReader(
    const katana::Uri& uri, uint64_t preload_start, uint64_t preload_end,
    std::shared_ptr<tsuba::FileView>* fv_ptr = nullptr) {
  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  if (auto res = fv->Bind(uri.string(), preload_start, preload_end, false);
      !res) {
    return res.error().WithContext("opening {}", uri);
  }

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  if (!open_file_result.ok()) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "arrow error: {}", open_file_result);
  }

  if (fv_ptr != nullptr) {
    *fv_ptr = fv;
  }
  return std::unique_ptr<parquet::arrow::FileReader>(std::move(reader));
}

}  // namespace

Result<std::unique_ptr<tsuba::ParquetReader>>
tsuba::ParquetReader::Make(ReadOpts opts) {
  return std::unique_ptr<ParquetReader>(
      new ParquetReader(opts.slice, opts.make_cannonical));
}

// Internal use only, invoke iff slice_ has a value
Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadFromUriSliced(const katana::Uri& uri) {
  auto slice = slice_.value();
  if (slice.offset < 0 || slice.length < 0) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "slice offset and length must be non-negative");
  }

  std::shared_ptr<FileView> fv;
  auto reader_res = MakeFileReader(uri, 0, 0, &fv);
  if (!reader_res) {
    return reader_res.error();
  }
  std::unique_ptr<parquet::arrow::FileReader> reader(
      std::move(reader_res.value()));

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
    return KATANA_ERROR(ErrorCode::ArrowError, "arrow error: {}", read_result);
  }

  out = out->Slice(row_offset, slice.length);

  return FixTable(std::move(out));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadTable(const katana::Uri& uri) {
  if (slice_) {
    // logic for a sliced read is different enough not to bother trying
    // to DRY these out
    return ReadFromUriSliced(uri);
  }

  auto reader_res =
      MakeFileReader(uri, 0, std::numeric_limits<uint64_t>::max());
  if (!reader_res) {
    return reader_res.error();
  }
  std::unique_ptr<parquet::arrow::FileReader> reader(
      std::move(reader_res.value()));

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadTable(&out);
  if (!read_result.ok()) {
    return KATANA_ERROR(ErrorCode::ArrowError, "arrow error: {}", read_result);
  }
  return FixTable(std::move(out));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::DoFilteredTableRead(
    parquet::arrow::FileReader* reader, const arrow::Schema& schema,
    const std::vector<int32_t>& indexes) {
  if (slice_) {
    // TODO(thunt) implement this
    return KATANA_ERROR(
        ErrorCode::NotImplemented,
        "sorry! missing support for sliced read when choosing columns");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
  std::unordered_map<int32_t, std::shared_ptr<arrow::ChunkedArray>> read_arrays;
  for (int32_t idx : indexes) {
    if (idx < 0) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument, "column indexes must be positive");
    }
    if (idx >= schema.num_fields()) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "column index {} should be less than the number of columns {}", idx,
          schema.num_fields());
    }
    std::shared_ptr<arrow::ChunkedArray> column;
    if (auto arr_it = read_arrays.find(idx); arr_it != read_arrays.end()) {
      column = arr_it->second;
    } else {
      auto status = reader->ReadColumn(idx, &column);
      if (!status.ok()) {
        return KATANA_ERROR(
            ErrorCode::ArrowError, "reading column {}: {}", idx, status);
      }
      read_arrays.emplace(idx, column);
    }
    fields.emplace_back(schema.field(idx));
    columns.emplace_back(std::move(column));
  }
  return FixTable(arrow::Table::Make(arrow::schema(fields), columns));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadColumn(const katana::Uri& uri, int32_t column_idx) {
  auto reader_res = MakeFileReader(uri, 0, 0);
  if (!reader_res) {
    return reader_res.error();
  }
  std::unique_ptr<parquet::arrow::FileReader> reader(
      std::move(reader_res.value()));

  std::shared_ptr<arrow::Schema> schema;
  auto status = reader->GetSchema(&schema);
  if (!status.ok()) {
    return KATANA_ERROR(ErrorCode::ArrowError, "reading schema: {}", status);
  }

  return DoFilteredTableRead(
      reader.get(), *schema, std::vector<int32_t>{column_idx});
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadTable(
    const katana::Uri& uri, const std::vector<int32_t>& column_indexes) {
  auto reader_res = MakeFileReader(uri, 0, 0);
  if (!reader_res) {
    return reader_res.error();
  }
  std::unique_ptr<parquet::arrow::FileReader> reader(
      std::move(reader_res.value()));

  std::shared_ptr<arrow::Schema> schema;
  auto status = reader->GetSchema(&schema);
  if (!status.ok()) {
    return KATANA_ERROR(ErrorCode::ArrowError, "reading schema: {}", status);
  }

  return DoFilteredTableRead(reader.get(), *schema, column_indexes);
}

Result<int32_t>
tsuba::ParquetReader::NumColumns(const katana::Uri& uri) {
  auto reader_res = MakeFileReader(uri, 0, 0);
  if (!reader_res) {
    return reader_res.error();
  }
  std::unique_ptr<parquet::arrow::FileReader> reader(
      std::move(reader_res.value()));

  std::shared_ptr<arrow::Schema> schema;
  auto status = reader->GetSchema(&schema);
  if (!status.ok()) {
    return KATANA_ERROR(ErrorCode::ArrowError, "reading schema: {}", status);
  }
  return schema->num_fields();
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::FixTable(std::shared_ptr<arrow::Table>&& _table) {
  std::shared_ptr<arrow::Table> table(std::move(_table));
  if (!make_cannonical_) {
    return table;
  }
  std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;
  arrow::SchemaBuilder schema_builder;
  for (int i = 0, size = table->num_columns(); i < size; ++i) {
    auto fixed_column_res = HandleBadParquetTypes(table->column(i));
    if (!fixed_column_res) {
      return fixed_column_res.error();
    }
    std::shared_ptr<arrow::ChunkedArray> fixed_column(
        std::move(fixed_column_res.value()));
    new_columns.emplace_back(fixed_column);
    auto new_field = std::make_shared<arrow::Field>(
        table->field(i)->name(), fixed_column->type());
    if (auto status = schema_builder.AddField(new_field); !status.ok()) {
      return KATANA_ERROR(
          ErrorCode::ArrowError, "fixing string type: {}", status);
    }
  }
  auto maybe_schema = schema_builder.Finish();
  if (!maybe_schema.ok()) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "finishing table schema: {}",
        maybe_schema.status());
  }
  std::shared_ptr<arrow::Schema> final_schema = maybe_schema.ValueOrDie();

  table = arrow::Table::Make(final_schema, new_columns);

  // Combine multiple chunks into one. Binary and string columns (c.f. large
  // binary and large string columns) are a special case. They may not be
  // combined into a single chunk due to the fact the offset type for these
  // columns is int32_t and thus the maximum size of an arrow::Array for these
  // types is 2^31.
  auto combine_result = table->CombineChunks(arrow::default_memory_pool());
  if (!combine_result.ok()) {
    return KATANA_ERROR(
        ErrorCode::ArrowError, "arrow error: {}", combine_result.status());
  }

  return combine_result.ValueOrDie();
}

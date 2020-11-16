#include "AddTables.h"

#include "tsuba/Errors.h"
#include "tsuba/FileView.h"

template <typename T>
using Result = galois::Result<T>;

namespace {

Result<std::shared_ptr<arrow::Table>>
DoLoadTable(const std::string& expected_name, const galois::Uri& file_path) {
  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  if (auto res = fv->Bind(file_path.string(), false); !res) {
    return res.error();
  }

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  if (!open_file_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", open_file_result);
    return tsuba::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadTable(&out);
  if (!read_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", read_result);
    return tsuba::ErrorCode::ArrowError;
  }

  // Combine multiple chunks into one. Binary and string columns (c.f. large
  // binary and large string columns) are a special case. They may not be
  // combined into a single chunk due to the fact the offset type for these
  // columns is int32_t and thus the maximum size of an arrow::Array for these
  // types is 2^31.
  auto combine_result = out->CombineChunks(arrow::default_memory_pool());
  if (!combine_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", combine_result.status());
    return tsuba::ErrorCode::ArrowError;
  }

  out = std::move(combine_result.ValueOrDie());

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    GALOIS_LOG_DEBUG("expected 1 field found {} instead", schema->num_fields());
    return tsuba::ErrorCode::InvalidArgument;
  }

  if (schema->field(0)->name() != expected_name) {
    GALOIS_LOG_DEBUG(
        "expected {} found {} instead", expected_name,
        schema->field(0)->name());
    return tsuba::ErrorCode::InvalidArgument;
  }

  return out;
}

Result<std::shared_ptr<arrow::Table>>
DoLoadTableSlice(
    const std::string& expected_name, const galois::Uri& file_path,
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
    GALOIS_LOG_DEBUG("arrow error: {}", open_file_result);
    return tsuba::ErrorCode::ArrowError;
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
    GALOIS_LOG_DEBUG("arrow error: {}", read_result);
    return tsuba::ErrorCode::ArrowError;
  }

  auto combine_result = out->CombineChunks(arrow::default_memory_pool());
  if (!combine_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", combine_result.status());
    return tsuba::ErrorCode::ArrowError;
  }

  out = std::move(combine_result.ValueOrDie());

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    GALOIS_LOG_DEBUG("expected 1 field found {} instead", schema->num_fields());
    return tsuba::ErrorCode::InvalidArgument;
  }

  if (schema->field(0)->name() != expected_name) {
    GALOIS_LOG_DEBUG(
        "expected {} found {} instead", expected_name,
        schema->field(0)->name());
    return tsuba::ErrorCode::InvalidArgument;
  }

  return out->Slice(row_offset, length);
}

}  // namespace

Result<std::shared_ptr<arrow::Table>>
tsuba::LoadTable(
    const std::string& expected_name, const galois::Uri& file_path) {
  try {
    return DoLoadTable(expected_name, file_path);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }
}

galois::Result<std::shared_ptr<arrow::Table>>
tsuba::LoadTableSlice(
    const std::string& expected_name, const galois::Uri& file_path,
    int64_t offset, int64_t length) {
  try {
    return DoLoadTableSlice(expected_name, file_path, offset, length);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return ErrorCode::ArrowError;
  }
}

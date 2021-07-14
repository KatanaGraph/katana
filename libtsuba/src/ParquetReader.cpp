#include "tsuba/ParquetReader.h"

#include <limits>
#include <memory>
#include <unordered_map>

#include <arrow/chunked_array.h>
#include <arrow/type.h>

#include "katana/JSON.h"
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
BuildReader(
    const std::string& uri, bool preload,
    std::shared_ptr<tsuba::FileView>* fv) {
  auto fv_tmp = std::make_shared<tsuba::FileView>();
  KATANA_CHECKED_CONTEXT(
      fv_tmp->Bind(
          uri, 0, preload ? std::numeric_limits<uint64_t>::max() : 0, false),
      "opening {}", uri);
  *fv = fv_tmp;

  std::unique_ptr<parquet::arrow::FileReader> reader;
  KATANA_CHECKED(
      parquet::arrow::OpenFile(fv_tmp, arrow::default_memory_pool(), &reader));

  return std::unique_ptr<parquet::arrow::FileReader>(std::move(reader));
}

Result<std::shared_ptr<arrow::Table>>
ReadTableSlice(
    parquet::arrow::FileReader* reader, tsuba::FileView* fv, int64_t first_row,
    int64_t last_row) {
  std::vector<int> row_groups;
  int rg_count = reader->num_row_groups();
  int64_t row_offset = 0;
  int64_t cumulative_rows = 0;
  int64_t file_offset = 0;
  int64_t cumulative_bytes = 0;

  for (int i = 0; cumulative_rows < last_row && i < rg_count; ++i) {
    auto rg_md = reader->parquet_reader()->metadata()->RowGroup(i);
    int64_t new_rows = rg_md->num_rows();
    int64_t new_bytes = rg_md->total_byte_size();
    if (first_row < cumulative_rows + new_rows) {
      if (row_groups.empty()) {
        row_offset = first_row - cumulative_rows;
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
  KATANA_CHECKED(reader->ReadRowGroups(row_groups, &out));
  return out->Slice(row_offset, last_row - first_row);
}

class BlockedParquetReader {
public:
  /// Read a potentially blocked Parquet file at the provide uri
  ///
  /// We consider 2 cases:
  ///  1) uri is a single parquet file
  ///  2) uri is a json file that contains a list of offsets
  ///
  /// We attempt 1) first and fall back on 2) if it fails.
  /// In both cases care is taken to read as few row groups and
  /// files as possible when accessing only metadata when preload
  /// is false. Setting preload to true will provide better performance
  /// when you know you're going to read everything.
  ///
  /// For 2) the json file contains a list of integers denoting table row
  /// offsets, indexes of this array inform the file names. For example
  /// a uri "s3://exmaple_file/table.parquet" that contains the json string
  /// "[0, 10]" corresponds to a single logical table who's rows 0-9 are in
  /// "s3://example_file/table.parquet.part_000000000" and rows 10-end are
  /// in "s3://example_file/table.parquet.part_000000001"
  static Result<std::unique_ptr<BlockedParquetReader>> Make(
      const katana::Uri& uri, bool preload) {
    std::shared_ptr<tsuba::FileView> fv;
    auto builder_res = BuildReader(uri.string(), preload, &fv);

    if (builder_res) {
      std::vector<std::unique_ptr<parquet::arrow::FileReader>> readers;
      std::vector<std::shared_ptr<tsuba::FileView>> fvs;
      readers.emplace_back(std::move(builder_res.value()));
      fvs.emplace_back(std::move(fv));

      return std::unique_ptr<BlockedParquetReader>(new BlockedParquetReader(
          uri.string(), std::move(fvs), std::move(readers), {0}));
    }

    if (builder_res.error() != katana::ErrorCode::InvalidArgument) {
      return builder_res.error();
    }

    // arrow parse failed, but it might be a list of offsets, try that
    std::vector<int64_t> row_offsets;

    KATANA_CHECKED(fv->Fill(0, std::numeric_limits<uint64_t>::max(), true));
    std::string raw_data(fv->ptr<char>(), fv->size());
    KATANA_CHECKED_CONTEXT(
        katana::JsonParse(raw_data, &row_offsets),
        "trying to parse invalid parquet as list of offsets");

    if (row_offsets.empty()) {
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument,
          "file must either be parquet data, or a json list of offsets");
    }

    std::vector<std::unique_ptr<parquet::arrow::FileReader>> readers(
        row_offsets.size());
    std::vector<std::shared_ptr<tsuba::FileView>> fvs(row_offsets.size());

    std::unique_ptr<BlockedParquetReader> bpr(new BlockedParquetReader(
        uri.string(), std::move(fvs), std::move(readers),
        std::move(row_offsets)));

    if (preload) {
      for (size_t i = 0, num_files = bpr->row_offsets_.size(); i < num_files;
           ++i) {
        KATANA_CHECKED(bpr->EnsureReader(i, true));
      }
    }
    return std::unique_ptr<BlockedParquetReader>(std::move(bpr));
  }

  Result<int64_t> NumRows() {
    size_t last_reader_idx = readers_.size() - 1;
    KATANA_CHECKED(EnsureReader(last_reader_idx));
    return row_offsets_[last_reader_idx] +
           readers_[last_reader_idx]->parquet_reader()->metadata()->num_rows();
  }

  Result<int32_t> NumColumns() {
    KATANA_CHECKED(EnsureReader(0));
    return readers_[0]->parquet_reader()->metadata()->num_columns();
  }

  Result<std::shared_ptr<arrow::Table>> ReadTable(
      std::optional<tsuba::ParquetReader::Slice> slice = std::nullopt) {
    if (!slice) {
      std::vector<std::shared_ptr<arrow::Table>> tables;
      for (size_t i = 0, num_files = readers_.size(); i < num_files; ++i) {
        KATANA_CHECKED(EnsureReader(i, true));
        std::shared_ptr<arrow::Table> table;
        KATANA_CHECKED(readers_[i]->ReadTable(&table));
        tables.emplace_back(std::move(table));
      }
      return KATANA_CHECKED(arrow::ConcatenateTables(tables));
    }

    int64_t curr_global_row = slice->offset;
    int64_t last_global_row =
        std::min(KATANA_CHECKED(NumRows()), curr_global_row + slice->length);

    if (last_global_row <= curr_global_row) {
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument,
          "slice would result in empty table");
    }

    auto it = std::lower_bound(
        row_offsets_.begin(), row_offsets_.end(), curr_global_row);
    size_t idx = std::distance(row_offsets_.begin(), it);

    std::vector<std::shared_ptr<arrow::Table>> tables;

    for (; idx < readers_.size() && curr_global_row < last_global_row; ++idx) {
      int64_t table_offset = row_offsets_[idx];
      int64_t next_table_offset =
          (idx == row_offsets_.size() - 1 ? std::numeric_limits<int64_t>::max()
                                          : row_offsets_[idx + 1]);
      std::shared_ptr<arrow::Table> table;
      if (curr_global_row == table_offset &&
          last_global_row >= next_table_offset) {
        KATANA_CHECKED(EnsureReader(idx, true));
        KATANA_CHECKED(readers_[idx]->ReadTable(&table));
      } else {
        KATANA_CHECKED(EnsureReader(idx, false));
        table = KATANA_CHECKED(ReadTableSlice(
            readers_[idx].get(), fvs_[idx].get(),
            curr_global_row - table_offset,
            std::min(
                next_table_offset - table_offset,
                last_global_row - table_offset)));
      }
      tables.emplace_back(std::move(table));
      curr_global_row = next_table_offset;
    }

    return KATANA_CHECKED(arrow::ConcatenateTables(tables));
  }

  Result<std::shared_ptr<arrow::Table>> ReadTable(
      std::vector<int32_t> col_indexes,
      std::optional<tsuba::ParquetReader::Slice> slice = std::nullopt) {
    if (slice) {
      return KATANA_ERROR(
          katana::ErrorCode::NotImplemented,
          "column subset read not implemented for slice! Sorry!");
    }

    std::vector<std::shared_ptr<arrow::Table>> tables;

    for (auto& reader : readers_) {
      std::vector<std::shared_ptr<arrow::Field>> fields;
      std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
      std::unordered_map<int32_t, std::shared_ptr<arrow::ChunkedArray>>
          read_arrays;

      std::shared_ptr<arrow::Schema> schema;
      KATANA_CHECKED(reader->GetSchema(&schema));
      for (int32_t idx : col_indexes) {
        if (idx < 0) {
          return KATANA_ERROR(
              ErrorCode::InvalidArgument, "column indexes must be positive");
        }
        if (idx >= schema->num_fields()) {
          return KATANA_ERROR(
              ErrorCode::InvalidArgument,
              "column index {} should be less than the number of columns {}",
              idx, schema->num_fields());
        }
        std::shared_ptr<arrow::ChunkedArray> column;
        if (auto arr_it = read_arrays.find(idx); arr_it != read_arrays.end()) {
          column = arr_it->second;
        } else {
          KATANA_CHECKED(reader->ReadColumn(idx, &column));
          read_arrays.emplace(idx, column);
        }
        fields.emplace_back(schema->field(idx));
        columns.emplace_back(std::move(column));
      }
      tables.emplace_back(arrow::Table::Make(arrow::schema(fields), columns));
    }
    return KATANA_CHECKED(arrow::ConcatenateTables(tables));
  }

private:
  BlockedParquetReader(
      std::string prefix, std::vector<std::shared_ptr<tsuba::FileView>>&& fvs,
      std::vector<std::unique_ptr<parquet::arrow::FileReader>>&& readers,
      std::vector<int64_t>&& row_offsets)
      : prefix_(std::move(prefix)),
        fvs_(std::move(fvs)),
        readers_(std::move(readers)),
        row_offsets_(std::move(row_offsets)) {}

  Result<void> EnsureReader(size_t idx, bool preload = false) {
    if (readers_[idx]) {
      KATANA_LOG_ASSERT(fvs_[idx]);
      return katana::ResultSuccess();
    }
    readers_[idx] = KATANA_CHECKED(BuildReader(
        fmt::format("{}.part_{:09}", prefix_, idx), preload, &fvs_[idx]));

    return katana::ResultSuccess();
  }

  std::string prefix_;
  std::vector<std::shared_ptr<tsuba::FileView>> fvs_;
  std::vector<std::unique_ptr<parquet::arrow::FileReader>> readers_;
  std::vector<int64_t> row_offsets_;
};

}  // namespace

Result<std::unique_ptr<tsuba::ParquetReader>>
tsuba::ParquetReader::Make(ReadOpts opts) {
  return std::unique_ptr<ParquetReader>(
      new ParquetReader(opts.slice, opts.make_cannonical));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadTable(const katana::Uri& uri) {
  bool preload = true;
  if (slice_) {
    if (slice_->offset < 0 || slice_->length < 0) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "slice offset and length must be non-negative");
    }
    preload = false;
  }

  auto bpr = KATANA_CHECKED(BlockedParquetReader::Make(uri, preload));
  return FixTable(KATANA_CHECKED(bpr->ReadTable(slice_)));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadColumn(const katana::Uri& uri, int32_t column_idx) {
  auto bpr = KATANA_CHECKED(BlockedParquetReader::Make(uri, false));
  return FixTable(KATANA_CHECKED(bpr->ReadTable({column_idx})));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadTable(
    const katana::Uri& uri, const std::vector<int32_t>& column_indexes) {
  auto bpr = KATANA_CHECKED(BlockedParquetReader::Make(uri, false));
  return FixTable(KATANA_CHECKED(bpr->ReadTable(column_indexes)));
}

Result<int32_t>
tsuba::ParquetReader::NumColumns(const katana::Uri& uri) {
  return KATANA_CHECKED(BlockedParquetReader::Make(uri, false))->NumColumns();
}

Result<int64_t>
tsuba::ParquetReader::NumRows(const katana::Uri& uri) {
  return KATANA_CHECKED(BlockedParquetReader::Make(uri, false))->NumRows();
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

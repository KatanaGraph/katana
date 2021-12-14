#include "tsuba/ParquetReader.h"

#include <limits>
#include <memory>
#include <unordered_map>

#include <arrow/array/util.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/cast.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <parquet/arrow/schema.h>

#include "katana/JSON.h"
#include "tsuba/Errors.h"
#include "tsuba/FileView.h"

template <typename T>
using Result = katana::Result<T>;

using ErrorCode = tsuba::ErrorCode;

namespace {

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
HandleBadParquetTypes(std::shared_ptr<arrow::ChunkedArray> old_array) {
  switch (old_array->type()->id()) {
  case arrow::Type::type::STRING: {
    auto opts = arrow::compute::CastOptions();
    opts.to_type = arrow::large_utf8();
    arrow::Datum cast_res =
        KATANA_CHECKED(arrow::compute::Cast(old_array, opts));
    return cast_res.chunked_array();
  }
  case arrow::Type::type::BINARY: {
    auto opts = arrow::compute::CastOptions();
    opts.to_type = arrow::large_binary();
    arrow::Datum cast_res =
        KATANA_CHECKED(arrow::compute::Cast(old_array, opts));
    return cast_res.chunked_array();
  }
  default:
    return old_array;
  }
}

katana::Result<std::shared_ptr<arrow::Field>>
HandleBadParquetTypes(std::shared_ptr<arrow::Field> old_field) {
  switch (old_field->type()->id()) {
  case arrow::Type::type::STRING: {
    return std::make_shared<arrow::Field>(
        old_field->name(), arrow::large_utf8());
  }
  default:
    return old_field;
  }
}

Result<std::unique_ptr<parquet::arrow::FileReader>>
BuildReader(
    const std::string& uri, bool preload,
    std::shared_ptr<tsuba::FileView>* fv) {
  auto fv_tmp = std::make_shared<tsuba::FileView>();
  uint64_t end = preload ? std::numeric_limits<uint64_t>::max() : 0;
  KATANA_CHECKED_CONTEXT(
      fv_tmp->Bind(uri, 0, end, false), "opening {}; begin: {}, end: {}", uri,
      0, end);
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

  Result<std::shared_ptr<arrow::Schema>> ReadSchema() {
    KATANA_CHECKED(EnsureReader(0));
    std::shared_ptr<arrow::Schema> schema;
    KATANA_CHECKED(parquet::arrow::FromParquetSchema(
        readers_[0]->parquet_reader()->metadata()->schema(), &schema));
    return schema;
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

    if (last_global_row < curr_global_row) {
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument,
          "slice cannot extend past end of table");
    }

    size_t idx = 0;
    for (; idx + 1 < row_offsets_.size(); ++idx) {
      if (curr_global_row < row_offsets_[idx + 1]) {
        break;
      }
    }

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

    if (tables.empty()) {
      KATANA_CHECKED(EnsureReader(0, false));
      std::shared_ptr<arrow::Schema> schema;
      KATANA_CHECKED(readers_[idx]->GetSchema(&schema));

      std::vector<std::shared_ptr<arrow::ChunkedArray>> cols;
      for (const auto& field : schema->fields()) {
        cols.emplace_back(std::make_shared<arrow::ChunkedArray>(
            KATANA_CHECKED(arrow::MakeArrayOfNull(field->type(), 0))));
      }
      return arrow::Table::Make(schema, cols);
    }

    return KATANA_CHECKED(arrow::ConcatenateTables(tables));
  }

  Result<std::shared_ptr<arrow::Table>> ReadTable(
      std::vector<int32_t> col_indexes,
      std::optional<tsuba::ParquetReader::Slice> slice = std::nullopt) {
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

    std::shared_ptr<arrow::Table> concatenated_table =
        KATANA_CHECKED(arrow::ConcatenateTables(tables));
    if (slice) {
      concatenated_table =
          concatenated_table->Slice(slice->offset, slice->length);
    }
    return concatenated_table;
  }

  Result<std::vector<std::string>> GetFiles() {
    std::vector<std::string> sub_files;
    sub_files.reserve(fvs_.size());
    // Bind some the file views so we can get the filenames
    for (size_t i = 0, num_files = readers_.size(); i < num_files; ++i) {
      KATANA_CHECKED(EnsureReader(i, false));
    }
    for (const auto& fv : fvs_) {
      sub_files.emplace_back(fv->filename());
    }
    return sub_files;
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
  return std::unique_ptr<ParquetReader>(new ParquetReader(opts.make_canonical));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadTable(
    const katana::Uri& uri, std::optional<tsuba::ParquetReader::Slice> slice) {
  bool preload = true;
  if (slice) {
    if (slice->offset < 0 || slice->length < 0) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "slice offset and length must be non-negative");
    }
    preload = false;
  }

  auto bpr = KATANA_CHECKED(BlockedParquetReader::Make(uri, preload));
  return FixTable(KATANA_CHECKED(bpr->ReadTable(slice)));
}

katana::Result<std::shared_ptr<arrow::Schema>>
tsuba::ParquetReader::GetSchema(const katana::Uri& uri) {
  auto bpr = KATANA_CHECKED(BlockedParquetReader::Make(uri, false));
  return FixSchema(KATANA_CHECKED(bpr->ReadSchema()));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadColumn(const katana::Uri& uri, int32_t column_idx) {
  auto bpr = KATANA_CHECKED(BlockedParquetReader::Make(uri, false));
  return FixTable(KATANA_CHECKED(bpr->ReadTable({column_idx})));
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::ReadTable(
    const katana::Uri& uri, const std::vector<int32_t>& column_indexes,
    std::optional<tsuba::ParquetReader::Slice> slice) {
  auto bpr = KATANA_CHECKED(BlockedParquetReader::Make(uri, false));
  return FixTable(KATANA_CHECKED(bpr->ReadTable(column_indexes, slice)));
}

Result<int32_t>
tsuba::ParquetReader::NumColumns(const katana::Uri& uri) {
  return KATANA_CHECKED(BlockedParquetReader::Make(uri, false))->NumColumns();
}

Result<int64_t>
tsuba::ParquetReader::NumRows(const katana::Uri& uri) {
  return KATANA_CHECKED(BlockedParquetReader::Make(uri, false))->NumRows();
}

Result<std::vector<std::string>>
tsuba::ParquetReader::GetFiles(const katana::Uri& uri) {
  return KATANA_CHECKED(BlockedParquetReader::Make(uri, false))->GetFiles();
}

Result<std::shared_ptr<arrow::Schema>>
tsuba::ParquetReader::FixSchema(const std::shared_ptr<arrow::Schema>& schema) {
  if (!make_canonical_) {
    return schema;
  }

  std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
  for (auto& field : fields) {
    field = KATANA_CHECKED(HandleBadParquetTypes(field));
  }
  return arrow::schema(fields);
}

Result<std::shared_ptr<arrow::Table>>
tsuba::ParquetReader::FixTable(std::shared_ptr<arrow::Table>&& _table) {
  std::shared_ptr<arrow::Table> table(std::move(_table));

  KATANA_CHECKED(table->Validate());

  if (!make_canonical_) {
    return table;
  }
  std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;
  arrow::SchemaBuilder schema_builder;
  for (int i = 0, size = table->num_columns(); i < size; ++i) {
    std::shared_ptr<arrow::ChunkedArray> fixed_column =
        KATANA_CHECKED(HandleBadParquetTypes(table->column(i)));
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

  // Combine multiple chunks into one. Binary and string columns (c.f.
  // binary and string columns) are a special case. They may not be
  // combined into a single chunk due to the fact the offset type for these
  // columns is int32_t and thus the maximum size of an arrow::Array for these
  // types is 2^31.
  table = KATANA_CHECKED(table->CombineChunks(arrow::default_memory_pool()));

  // lots of the code base assumes chunks will exist, but arrow allows zero length
  // chunked arrays to have zero chunks. Let's be helpful.
  if (table->num_rows() == 0) {
    auto columns = table->columns();
    auto schema = table->schema();

    for (auto& col : columns) {
      if (col->num_chunks() == 0) {
        col = std::make_shared<arrow::ChunkedArray>(
            KATANA_CHECKED(arrow::MakeArrayOfNull(col->type(), 0)));
      }
    }

    table = arrow::Table::Make(schema, columns);
  }

  return table;
}

#include "tsuba/ParquetWriter.h"

#include "katana/ArrowInterchange.h"
#include "katana/JSON.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"

template <typename T>
using Result = katana::Result<T>;

namespace {

// this value was determined empirically
constexpr int64_t kMaxRowsPerFile = 0x3FFFFFFE;

uint64_t
EstimateElementSize(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  uint64_t cumulative_size = 0;
  for (const auto& chunk : chunked_array->chunks()) {
    cumulative_size += katana::ApproxArrayMemUse(chunk);
  }
  return cumulative_size / chunked_array->length();
}

uint64_t
EstimateRowSize(const std::shared_ptr<arrow::Table>& table) {
  uint64_t row_size = 0;
  for (const auto& col : table->columns()) {
    row_size += EstimateElementSize(col);
  }
  return row_size;
}

constexpr uint64_t kMB = 1UL << 20;

std::vector<std::shared_ptr<arrow::Table>>
BlockTable(std::shared_ptr<arrow::Table> table, uint64_t mbs_per_block) {
  if (table->num_rows() <= 1) {
    return {table};
  }
  uint64_t row_size = EstimateRowSize(table);
  uint64_t block_size = mbs_per_block * kMB;

  if (row_size * table->num_rows() < block_size) {
    return {std::move(table)};
  }
  int64_t rows_per_block = (block_size + row_size - 1) / row_size;

  std::vector<std::shared_ptr<arrow::Table>> blocks;
  int64_t row_idx = 0;
  int64_t num_rows = table->num_rows();
  while (row_idx < table->num_rows()) {
    blocks.emplace_back(
        table->Slice(row_idx, std::min(num_rows - row_idx, rows_per_block)));
    row_idx += rows_per_block;
  }
  return blocks;
}

Result<void>
DoStoreParquet(
    const std::string& path, std::shared_ptr<arrow::Table> table,
    const std::shared_ptr<parquet::WriterProperties>& writer_props,
    const std::shared_ptr<parquet::ArrowWriterProperties>& arrow_props,
    tsuba::WriteGroup* desc) {
  auto ff = std::make_shared<tsuba::FileFrame>();
  KATANA_CHECKED(ff->Init());
  ff->Bind(path);

  auto future = std::async(
      std::launch::async,
      [table = std::move(table), ff = std::move(ff), desc, writer_props,
       arrow_props]() mutable -> katana::CopyableResult<void> {
        auto write_result = parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), ff,
            std::numeric_limits<int64_t>::max(), writer_props, arrow_props);
        table.reset();

        if (!write_result.ok()) {
          return KATANA_ERROR(
              tsuba::ErrorCode::ArrowError, "arrow error: {}", write_result);
        }
        if (desc) {
          desc->AddToOutstanding(ff->map_size());
        }

        TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
        KATANA_CHECKED(ff->Persist());

        return katana::CopyableResultSuccess();
      });

  if (!desc) {
    KATANA_CHECKED(future.get());
    return katana::ResultSuccess();
  }

  desc->AddOp(std::move(future), path);
  return katana::ResultSuccess();
}

}  // namespace

Result<std::unique_ptr<tsuba::ParquetWriter>>
tsuba::ParquetWriter::Make(
    const std::shared_ptr<arrow::ChunkedArray>& array, const std::string& name,
    WriteOpts opts) {
  return Make(
      arrow::Table::Make(
          arrow::schema({arrow::field(name, array->type())}), {array}),
      opts);
}

Result<std::unique_ptr<tsuba::ParquetWriter>>
tsuba::ParquetWriter::Make(
    std::shared_ptr<arrow::Table> table, WriteOpts opts) {
  if (!opts.write_blocked) {
    return std::unique_ptr<ParquetWriter>(
        new ParquetWriter({std::move(table)}, opts));
  }
  return std::unique_ptr<ParquetWriter>(new ParquetWriter(
      BlockTable(std::move(table), opts.mbs_per_block), opts));
}

katana::Result<void>
tsuba::ParquetWriter::WriteToUri(const katana::Uri& uri, WriteGroup* group) {
  try {
    return StoreParquet(uri, group);
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

std::shared_ptr<parquet::WriterProperties>
tsuba::ParquetWriter::StandardWriterProperties() {
  return parquet::WriterProperties::Builder()
      .version(opts_.parquet_version)
      ->data_page_version(opts_.data_page_version)
      ->build();
}

std::shared_ptr<parquet::ArrowWriterProperties>
tsuba::ParquetWriter::StandardArrowProperties() {
  return parquet::ArrowWriterProperties::Builder().build();
}

/// Store the arrow table in a file
katana::Result<void>
tsuba::ParquetWriter::StoreParquet(
    std::shared_ptr<arrow::Table> table, const katana::Uri& uri,
    tsuba::WriteGroup* desc) {
  auto writer_props = StandardWriterProperties();
  auto arrow_props = StandardArrowProperties();
  std::string prefix = uri.string();

  if (table->num_rows() <= kMaxRowsPerFile) {
    return DoStoreParquet(prefix, table, writer_props, arrow_props, desc);
  }

  std::vector<std::shared_ptr<arrow::Table>> tables;
  std::vector<int64_t> table_offsets;

  // Slicing like this is necessary because of a problem with arrow<>parquet
  // and nulls for string columns. If entries in a column are all or mostly null
  // and greater than the element limit for a String array, you can end up
  // in a situation where you've generated a parquet file that arrow cannot
  // read. To make sure we don't end up in that situation, slice the table here
  // into groups of rows that are definitely smaller than the element limit
  for (int64_t i = 0, total_rows = table->num_rows(); i < total_rows;
       i += kMaxRowsPerFile) {
    table_offsets.emplace_back(i);
    tables.emplace_back(table->Slice(i, kMaxRowsPerFile));
  }
  table.reset();

  uint32_t table_count = 0;
  for (const auto& t : tables) {
    KATANA_CHECKED(DoStoreParquet(
        fmt::format("{}.part_{:09}", prefix, table_count++), t, writer_props,
        arrow_props, desc));
  }
  return FileStore(
      uri.string(), KATANA_CHECKED(katana::JsonDump(table_offsets)));
}

katana::Result<void>
tsuba::ParquetWriter::StoreParquet(
    const katana::Uri& uri, tsuba::WriteGroup* desc) {
  if (!opts_.write_blocked) {
    KATANA_LOG_ASSERT(tables_.size() == 1);
    return StoreParquet(tables_[0], uri, desc);
  }

  std::unique_ptr<tsuba::WriteGroup> our_desc;
  if (!desc) {
    our_desc = KATANA_CHECKED(WriteGroup::Make());
    desc = our_desc.get();
  }

  katana::Result<void> ret = katana::ResultSuccess();
  for (uint64_t i = 0, num_tables = tables_.size(); i < num_tables; ++i) {
    ret = StoreParquet(tables_[i], uri + fmt::format(".{:06}", i), desc);
    if (!ret) {
      break;
    }
  }

  if (desc == our_desc.get()) {
    auto final_ret = desc->Finish();
    if (!final_ret && !ret) {
      KATANA_LOG_ERROR("multiple errors, masking: {}", final_ret.error());
      return ret;
    }
    ret = final_ret;
  }
  return ret;
}

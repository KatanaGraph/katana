#include "tsuba/ParquetWriter.h"

#include "katana/ArrowInterchange.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"

template <typename T>
using Result = katana::Result<T>;

namespace {

// constant taken directly from the arrow docs
constexpr uint64_t kMaxStringChunkSize = 0x7FFFFFFE;

katana::Result<std::vector<std::shared_ptr<arrow::Array>>>
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

  std::shared_ptr<arrow::Array> new_arr;
  auto status = builder.Finish(&new_arr);
  if (!status.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "finishing string array: {}", status);
  }
  if (new_arr->length() > 0) {
    chunks.emplace_back(new_arr);
  }
  return chunks;
}

// HandleBadParquetTypes here and HandleBadParquetTypes in ParquetReader.cpp
// workaround a libarrow2.0 limitation in reading and writing LargeStrings to
// parquet files.
katana::Result<std::shared_ptr<arrow::ChunkedArray>>
HandleBadParquetTypes(std::shared_ptr<arrow::ChunkedArray> old_array) {
  switch (old_array->type()->id()) {
  case arrow::Type::type::LARGE_STRING: {
    std::vector<std::shared_ptr<arrow::Array>> new_chunks;
    for (const auto& chunk : old_array->chunks()) {
      auto arr = std::static_pointer_cast<arrow::LargeStringArray>(chunk);
      auto new_chunk_res = LargeStringToChunkedString(arr);
      if (!new_chunk_res) {
        return new_chunk_res.error();
      }
      const auto& chunks = new_chunk_res.value();
      new_chunks.insert(new_chunks.end(), chunks.begin(), chunks.end());
    }

    auto maybe_res = arrow::ChunkedArray::Make(new_chunks, arrow::utf8());
    if (!maybe_res.ok()) {
      return KATANA_ERROR(
          tsuba::ErrorCode::ArrowError, "building chunked array: {}",
          maybe_res.status());
    }
    return maybe_res.ValueOrDie();
  }
  default:
    return old_array;
  }
}

katana::Result<std::shared_ptr<arrow::Table>>
HandleBadParquetTypes(const std::shared_ptr<arrow::Table>& old_table) {
  std::vector<std::shared_ptr<arrow::ChunkedArray>> new_arrays;
  std::vector<std::shared_ptr<arrow::Field>> new_fields;

  for (int i = 0, size = old_table->num_columns(); i < size; ++i) {
    auto new_array_res = HandleBadParquetTypes(old_table->column(i));
    if (!new_array_res) {
      return new_array_res.error();
    }
    std::shared_ptr<arrow::ChunkedArray> new_array =
        std::move(new_array_res.value());
    auto old_field = old_table->field(i);
    new_fields.emplace_back(
        std::make_shared<arrow::Field>(old_field->name(), new_array->type()));
    new_arrays.emplace_back(new_array);
  }
  return arrow::Table::Make(arrow::schema(new_fields), new_arrays);
}

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
  auto ff = std::make_shared<tsuba::FileFrame>();
  if (auto res = ff->Init(); !res) {
    return res.error().WithContext("creating output buffer");
  }
  ff->Bind(uri.string());
  auto future = std::async(
      std::launch::async,
      [table = std::move(table), ff = std::move(ff), desc,
       writer_props = StandardWriterProperties(),
       arrow_props = StandardArrowProperties()]() mutable
      -> katana::CopyableResult<void> {
        auto res = HandleBadParquetTypes(table);
        if (!res) {
          return res.error().WithContext(
              "conversion from arrow to parquet mismatch");
        }
        table = std::move(res.value());
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
        if (auto res = ff->Persist(); !res) {
          return res.error();
        }
        return katana::CopyableResultSuccess();
      });

  if (!desc) {
    auto res = future.get();
    if (!res) {
      return res.error();
    }
    return katana::ResultSuccess();
  }

  desc->AddOp(std::move(future), uri.string());
  return katana::ResultSuccess();
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
    auto desc_res = WriteGroup::Make();
    if (!desc_res) {
      return desc_res.error();
    }
    our_desc = std::move(desc_res.value());
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

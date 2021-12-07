#ifndef KATANA_LIBTSUBA_TSUBA_PARQUETWRITER_H_
#define KATANA_LIBTSUBA_TSUBA_PARQUETWRITER_H_

#include <limits>
#include <vector>

#include <arrow/api.h>
#include <parquet/properties.h>

#include "katana/Result.h"
#include "katana/URI.h"
#include "tsuba/WriteGroup.h"

namespace tsuba {

class KATANA_EXPORT ParquetWriter {
public:
  struct WriteOpts {
    /// int64 timestamps with nanosecond resolution requires Parquet version
    /// 2.0. In Arrow to Parquet version 1.0, nanosecond timestamps will get
    /// truncated to milliseconds.
    parquet::ParquetVersion::type parquet_version{
        parquet::ParquetVersion::PARQUET_2_6};
    parquet::ParquetDataPageVersion data_page_version{
        parquet::ParquetDataPageVersion::V2};

    /// if true, write operations will produce multiple files (improves
    /// available parallelism. Files will have the extension `.i` where i
    /// represents the ith block of the table
    bool write_blocked{false};

    /// control the approximate size of blocked files when writing blocked
    uint64_t mbs_per_block{256};
    static WriteOpts Defaults() { return WriteOpts{}; }
  };

  /// \returns a Writer that will write a table consisting of a single column
  /// \param array will become the lone column in the table
  /// \param name will become the name of the column in the table
  /// \param opts controls things like output format
  static katana::Result<std::unique_ptr<ParquetWriter>> Make(
      const std::shared_ptr<arrow::ChunkedArray>& array,
      const std::string& name, WriteOpts opts = WriteOpts::Defaults());

  /// \returns a Writer that will write a table to a storage location
  /// \param table the table to be written out
  /// \param opts controls things like output format
  static katana::Result<std::unique_ptr<ParquetWriter>> Make(
      std::shared_ptr<arrow::Table> table,
      WriteOpts opts = WriteOpts::Defaults());

  /// write table out to a storage location. If `group` is null,
  /// the write is synchronous, if not an asynchronous write is started to be
  /// managed by group
  /// \param uri the storage location to write to
  /// \param group optional write group to group this write operation with
  katana::Result<void> WriteToUri(
      const katana::Uri& uri, WriteGroup* group = nullptr);

private:
  ParquetWriter(
      std::vector<std::shared_ptr<arrow::Table>> tables, WriteOpts opts)
      : tables_(std::move(tables)), opts_(opts) {}

  std::shared_ptr<parquet::WriterProperties> StandardWriterProperties();

  std::shared_ptr<parquet::ArrowWriterProperties> StandardArrowProperties();

  katana::Result<void> StoreParquet(
      const katana::Uri& uri, tsuba::WriteGroup* desc);

  katana::Result<void> StoreParquet(
      std::shared_ptr<arrow::Table> table, const katana::Uri& uri,
      tsuba::WriteGroup* desc);

  std::vector<std::shared_ptr<arrow::Table>> tables_;
  WriteOpts opts_;
};

}  // namespace tsuba

#endif

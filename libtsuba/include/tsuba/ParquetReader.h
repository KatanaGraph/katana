#ifndef KATANA_LIBTSUBA_TSUBA_PARQUETREADER_H_
#define KATANA_LIBTSUBA_TSUBA_PARQUETREADER_H_

#include <optional>

#include <arrow/api.h>

#include "katana/Result.h"
#include "katana/Uri.h"

namespace parquet::arrow {

class FileReader;

}  // namespace parquet::arrow

namespace tsuba {

class KATANA_EXPORT ParquetReader {
public:
  struct Slice {
    int64_t offset;
    int64_t length;
  };

  struct ReadOpts {
    /// if true (default) make sure canonical types are used and table columns
    /// are not chunked
    bool make_cannonical{true};

    /// if provided, slice the resulting table so that it only contains
    /// Slice.length rows starting from Slice.offset
    std::optional<Slice> slice{std::nullopt};

    static ReadOpts Defaults() { return ReadOpts{}; }
  };

  /// build a reader that will read a table from storage location optionally
  /// reading only part of the table.
  /// \param opts an opt structure detailing how reads should behave (see
  ///    the ReadOpts struct definition for details)
  static katana::Result<std::unique_ptr<ParquetReader>> Make(
      ReadOpts opts = ReadOpts::Defaults());

  /// read table from storage
  ///   \param uri an identifier for a parquet file
  katana::Result<std::shared_ptr<arrow::Table>> ReadTable(
      const katana::Uri& uri);

  /// read part of a table from storage
  /// n.b. support for the `slice` read option is missing here
  ///   \param uri an identifier for a parquet file
  ///   \param column_bitmap must have the same length as the number of columns
  ///      in the table in the parquet file. The loaded table will only contain
  ///      columns at indexes that are true in the bitmap
  katana::Result<std::shared_ptr<arrow::Table>> ReadTable(
      const katana::Uri& uri, const std::vector<int32_t>& column_bitmap);

  /// read a column part of a table from storage
  /// n.b. support for the `slice` read option is missing here
  ///   \param uri an identifier for a parquet file
  ///   \param column_idx must be a valid column index for the table in that
  ///      file
  katana::Result<std::shared_ptr<arrow::Table>> ReadColumn(
      const katana::Uri& uri, int32_t column_idx);

  /// Get the number of columns for the table stored in a parquet file
  ///   \param uri an identifier for a parquet file
  katana::Result<int32_t> NumColumns(const katana::Uri& uri);

  /// Get the number of rows for the table stored in a parquet file
  ///   \param uri an identifier for a parquet file
  katana::Result<int64_t> NumRows(const katana::Uri& uri);

private:
  ParquetReader(std::optional<Slice> slice, bool make_cannonical)
      : slice_(slice), make_cannonical_{make_cannonical} {}

  katana::Result<std::shared_ptr<arrow::Table>> ReadFromUriSliced(
      const katana::Uri& uri);

  katana::Result<std::shared_ptr<arrow::Table>> FixTable(
      std::shared_ptr<arrow::Table>&& _table);

  katana::Result<std::shared_ptr<arrow::Table>> DoFilteredTableRead(
      parquet::arrow::FileReader* reader, const arrow::Schema& schema,
      const std::vector<int32_t>& filter);

  std::optional<Slice> slice_;
  bool make_cannonical_;
};

}  // namespace tsuba

#endif

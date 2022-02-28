#ifndef KATANA_LIBTSUBA_KATANA_PARQUETREADER_H_
#define KATANA_LIBTSUBA_KATANA_PARQUETREADER_H_

#include <optional>

#include <arrow/api.h>

#include "katana/Result.h"
#include "katana/URI.h"

namespace parquet::arrow {

class FileReader;

}  // namespace parquet::arrow

namespace katana {

class KATANA_EXPORT ParquetReader {
public:
  struct Slice {
    int64_t offset;
    int64_t length;
  };

  struct ReadOpts {
    /// if true (default) make sure canonical types are used and table columns
    /// are not chunked
    bool make_canonical{true};

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
      const katana::URI& uri, std::optional<Slice> slice = std::nullopt);

  /// read part of a table from storage
  ///   \param uri an identifier for a parquet file
  ///   \param column_bitmap must have the same length as the number of columns
  ///      in the table in the parquet file. The loaded table will only contain
  ///      columns at indexes that are true in the bitmap
  katana::Result<std::shared_ptr<arrow::Table>> ReadTable(
      const katana::URI& uri, const std::vector<int32_t>& column_bitmap,
      std::optional<Slice> slice = std::nullopt);

  /// read only the schema from a parquet file in storage
  katana::Result<std::shared_ptr<arrow::Schema>> GetSchema(
      const katana::URI& uri);

  /// read a column part of a table from storage
  /// n.b. support for the `slice` read option is missing here
  ///   \param uri an identifier for a parquet file
  ///   \param column_idx must be a valid column index for the table in that
  ///      file
  katana::Result<std::shared_ptr<arrow::Table>> ReadColumn(
      const katana::URI& uri, int32_t column_idx);

  /// Get the number of columns for the table stored in a parquet file
  ///   \param uri an identifier for a parquet file
  katana::Result<int32_t> NumColumns(const katana::URI& uri);

  /// Get the number of rows for the table stored in a parquet file
  ///   \param uri an identifier for a parquet file
  katana::Result<int64_t> NumRows(const katana::URI& uri);

  /// Get the files for the logical parquet table
  ///   \param uri an identifier for a parquet file
  katana::Result<std::vector<std::string>> GetFiles(const katana::URI& uri);

private:
  ParquetReader(bool make_canonical) : make_canonical_{make_canonical} {}

  katana::Result<std::shared_ptr<arrow::Table>> ReadFromUriSliced(
      const katana::URI& uri);

  katana::Result<std::shared_ptr<arrow::Table>> FixTable(
      std::shared_ptr<arrow::Table>&& _table);

  katana::Result<std::shared_ptr<arrow::Schema>> FixSchema(
      const std::shared_ptr<arrow::Schema>& schema);

  bool make_canonical_;
};

}  // namespace katana

#endif

#ifndef KATANA_LIBGLUON_KATANA_STORAGEBACKEDARROWTABLE_H_
#define KATANA_LIBGLUON_KATANA_STORAGEBACKEDARROWTABLE_H_

#include <memory>
#include <unordered_map>

#include <arrow/api.h>

#include "katana/ErrorCode.h"
#include "katana/StorageBackedArrowArray.h"
#include "katana/WriteGroup.h"

namespace katana {

// TODO(thunt): move this to tsuba

class KATANA_EXPORT StorageBackedArrowTable {
public:
  StorageBackedArrowTable(const StorageBackedArrowTable& no_copy) = delete;
  StorageBackedArrowTable& operator=(const StorageBackedArrowTable& no_copy) =
      delete;
  StorageBackedArrowTable(StorageBackedArrowTable&& no_move) = delete;
  StorageBackedArrowTable& operator=(StorageBackedArrowTable&& no_move) =
      delete;

  ~StorageBackedArrowTable() = default;

  /// make a new table with no columns, but an initial number of rows
  /// when other columns are appended these indexes will become nulls
  static Result<std::shared_ptr<StorageBackedArrowTable>> Make(
      const URI& storage_location, int64_t rows = 0);

  static Result<std::shared_ptr<StorageBackedArrowTable>> Make(
      const URI& storage_location, const std::vector<std::string>& names,
      const std::vector<std::shared_ptr<StorageBackedArrowArray>>& cols);

  static Result<std::shared_ptr<StorageBackedArrowTable>> Make(
      const URI& storage_location, const std::vector<std::string>& names,
      const std::vector<std::shared_ptr<arrow::ChunkedArray>>& cols) {
    std::vector<std::shared_ptr<StorageBackedArrowArray>> wrapped_cols;
    wrapped_cols.reserve(cols.size());
    for (const auto& col : cols) {
      wrapped_cols.emplace_back(
          KATANA_CHECKED(StorageBackedArrowArray::Make(storage_location, col)));
    }
    return Make(storage_location, names, wrapped_cols);
  }

  static Result<std::shared_ptr<StorageBackedArrowTable>> Make(
      const URI& storage_location,
      const std::vector<std::shared_ptr<arrow::Field>>& fields,
      const std::vector<std::shared_ptr<arrow::ChunkedArray>>& cols) {
    std::vector<std::string> names;
    names.reserve(fields.size());
    for (const auto& field : fields) {
      names.emplace_back(field->name());
    }
    return Make(storage_location, names, cols);
  }

  static Result<std::shared_ptr<StorageBackedArrowTable>> Make(
      const URI& storage_location, const std::shared_ptr<arrow::Table>& table) {
    return Make(storage_location, table->schema()->fields(), table->columns());
  }

  static std::future<CopyableResult<std::shared_ptr<StorageBackedArrowTable>>>
  FromStorageAsync(const URI& uri);

  static Result<std::shared_ptr<StorageBackedArrowTable>> FromStorage(
      const URI& uri) {
    return KATANA_CHECKED(FromStorageAsync(uri).get());
  }

  /// return a new table with the columns in to_append appended
  /// columns in this table with matching names will be extended with content
  ///   from to_append, if the input has columns that do not match, this will
  ///   retroactively create null columns the size of this table to append them
  ///   to
  /// if take_indexes is null, other columns will be lengthened with null values
  /// if take_indexes is not null, its length must match the number of rows in
  ///    to append, and other columns will be lengthened with values taken from
  ///    those indexes
  Result<std::shared_ptr<StorageBackedArrowTable>> Append(
      const std::shared_ptr<arrow::Table>& to_append,
      const std::shared_ptr<arrow::Array>& take_indexes = nullptr);
  Result<std::shared_ptr<StorageBackedArrowTable>> Append(
      const std::shared_ptr<StorageBackedArrowTable>& to_append,
      const std::shared_ptr<arrow::Array>& take_indexes = nullptr);

  /// Just copy and append data of all columns
  Result<std::shared_ptr<StorageBackedArrowTable>> TakeAppend(
      const std::shared_ptr<arrow::Array>& take_indexes) {
    return Append(std::shared_ptr<StorageBackedArrowTable>(), take_indexes);
  }

  Result<std::shared_ptr<StorageBackedArrowTable>> AppendNulls(
      int64_t num_nulls);

  bool HasColumn(const std::string& name) const {
    return (columns_.find(name) != columns_.end());
  }

  // return a new table with null_count nulls appended to each row
  Result<std::shared_ptr<arrow::ChunkedArray>> GetColumn(
      const std::string& name, bool un_chunk = true) {
    return KATANA_CHECKED(LookupColumn(name))->GetArray(un_chunk);
  }

  int64_t num_rows() const { return num_rows_; };
  int num_columns() const { return static_cast<int>(columns_.size()); };
  const std::shared_ptr<arrow::Schema>& schema() const { return schema_; };

  Result<void> UnloadColumn(const std::string& name, WriteGroup* wg) {
    return KATANA_CHECKED(LookupColumn(name))->Unload(wg);
  }

  Result<void> Unload(WriteGroup* wg = nullptr);

  std::future<CopyableResult<void>> LoadColumnAsync(const std::string& name) {
    return std::async(std::launch::async, [=]() -> CopyableResult<void> {
      // materializes the array
      KATANA_CHECKED(KATANA_CHECKED(LookupColumn(name))->GetArray());
      return CopyableResultSuccess();
    });
  }

  /// Store all columns, and return a uri to a flat buffer struct that
  /// describes them
  ///
  /// If provided with the optional write group, writes will be added to the
  /// group to overlap them; use the wait group to make sure writing succeeds
  /// in that case
  Result<URI> Persist(WriteGroup* wg = nullptr);

private:
  StorageBackedArrowTable(URI storage_location, int64_t num_rows)
      : storage_location_(std::move(storage_location)), num_rows_(num_rows) {}

  static std::shared_ptr<StorageBackedArrowTable> MakeShared(
      URI storage_location, int64_t num_rows) {
    return std::shared_ptr<StorageBackedArrowTable>(
        new StorageBackedArrowTable(std::move(storage_location), num_rows));
  }

  Result<std::shared_ptr<StorageBackedArrowTable>> AppendNewData(
      const std::shared_ptr<arrow::Table>& to_append);
  Result<std::shared_ptr<StorageBackedArrowTable>> AppendNewData(
      const std::shared_ptr<StorageBackedArrowTable>& to_append);

  Result<std::shared_ptr<StorageBackedArrowArray>> LookupColumn(
      const std::string& name) {
    auto it = columns_.find(name);
    if (it == columns_.end()) {
      return KATANA_ERROR(ErrorCode::NotFound, "no column with that name");
    }
    return it->second;
  }

  template <typename TableType>
  Result<std::shared_ptr<StorageBackedArrowTable>> AppendCommon(
      const std::shared_ptr<TableType>& to_append,
      const std::shared_ptr<arrow::Array>& take_indexes);

  Result<void> FillOtherColumns(
      StorageBackedArrowTable* table,
      const std::shared_ptr<arrow::Array>& take_indexes = nullptr);

  Result<void> ResetSchema();

  URI storage_location_;

  std::unordered_map<std::string, std::shared_ptr<StorageBackedArrowArray>>
      columns_;

  std::shared_ptr<arrow::Schema> schema_;

  int64_t num_rows_;
};

}  // namespace katana

#endif

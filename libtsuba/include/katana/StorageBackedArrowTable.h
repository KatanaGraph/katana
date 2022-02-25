#ifndef KATANA_LIBTSUBA_KATANA_STORAGEBACKEDARROWTABLE_H_
#define KATANA_LIBTSUBA_KATANA_STORAGEBACKEDARROWTABLE_H_

#include <memory>
#include <set>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "katana/ErrorCode.h"
#include "katana/StorageBackedArrowArray.h"
#include "katana/WriteGroup.h"

namespace katana {

// TODO(thunt): move this to tsuba

class KATANA_EXPORT StorageBackedArrowTable {
  using ColumnMap =
      std::unordered_map<std::string, std::shared_ptr<StorageBackedArrowArray>>;

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

  /// Register what column names and indices for rows an iterator will return.
  /// Currently, only iterators respect deferred take
  /// Pass an empty set and a nullptr to reset a deferred take.
  void DeferredTake(
      std::set<std::string>&& names,
      const std::shared_ptr<arrow::Array>& take_indexes) {
    deferred_take_names_ = std::move(names);
    deferred_take_indexes_ = std::move(take_indexes);
  }

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

  /// An iterator that first returns in memory columns, then returns on-storage
  /// columns.
  // TODO(witchel) add prefetching and group reads
  class iterator {
  public:
    iterator(StorageBackedArrowTable& sbat)
        : in_mem_pass_(true),
          colmap_ref_(sbat.columns_),
          deferred_take_names_(sbat.deferred_take_names_) {
      deferred_take_indexes_ = sbat.deferred_take_indexes_;
      it_ = colmap_ref_.begin();
    }
    static iterator MakeEnd(StorageBackedArrowTable& sbat) {
      auto it = iterator(sbat);
      it.in_mem_pass_ = false;
      it.it_ = it.colmap_ref_.end();
      return it;
    }

    // Use an iterator over the column map, but do it twice, first for
    // in-memory arrays
    iterator operator++() {
      if (in_mem_pass_) {
        while (++it_ != colmap_ref_.end() &&
               (deferred_take_names_.empty() ||
                deferred_take_names_.find(it_->first) !=
                    deferred_take_names_.end())) {
          if (it_->second->IsMaterialized()) {
            visited.insert(it_->first);
            return *this;
          }
        }
        it_ = colmap_ref_.begin();
      }
      in_mem_pass_ = false;
      if (it_ == colmap_ref_.end()) {
        return *this;
      }
      while (++it_ != colmap_ref_.end() &&
             visited.find(it_->first) == visited.end() &&
             (deferred_take_names_.empty() ||
              deferred_take_names_.find(it_->first) !=
                  deferred_take_names_.end())) {
        visited.insert(it_->first);
        return *this;
      }
      return *this;
    }
    bool operator!=(const iterator& other) const {
      return (in_mem_pass_ == false) && it_ != other.it_;
    }
    std::pair<std::string, std::shared_ptr<arrow::Array>> operator*() const {
      auto res = it_->second->GetArray(/*de_chunk*/ true);
      if (!res) {
        KATANA_LOG_WARN("iterator error {}", res.error());
        return {"error", nullptr};
      }
      auto arr = res.value()->chunk(0);
      if (deferred_take_indexes_) {
        auto arrow_res = arrow::compute::Take(
            arr, deferred_take_indexes_,
            arrow::compute::TakeOptions::BoundsCheck());
        if (!arrow_res.ok()) {
          KATANA_LOG_WARN("iterator error {}", arrow_res.status());
          return {"error", nullptr};
        }
        arr = arrow_res.ValueOrDie().make_array();
      }
      return {it_->first, arr};
    }

  private:
    bool in_mem_pass_{true};
    std::set<std::string> visited;
    ColumnMap::iterator it_;
    ColumnMap& colmap_ref_;
    const std::set<std::string>& deferred_take_names_;
    std::shared_ptr<arrow::Array> deferred_take_indexes_;
  };
  iterator begin() { return iterator(*this); }
  iterator end() { return iterator::MakeEnd(*this); }

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

  ColumnMap columns_;

  std::shared_ptr<arrow::Schema> schema_;

  int64_t num_rows_;

  std::set<std::string> deferred_take_names_;
  std::shared_ptr<arrow::Array> deferred_take_indexes_;
};

}  // namespace katana

#endif

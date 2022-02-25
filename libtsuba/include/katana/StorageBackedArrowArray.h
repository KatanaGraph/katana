#ifndef KATANA_LIBTSUBA_KATANA_STORAGEBACKEDARROWARRAY_H_
#define KATANA_LIBTSUBA_KATANA_STORAGEBACKEDARROWARRAY_H_

#include <list>
#include <memory>

#include <arrow/api.h>
#include <flatbuffers/flatbuffers.h>

#include "katana/LazyArrowArray.h"
#include "katana/ParquetReader.h"
#include "katana/PreparedUpdate.h"
#include "katana/Result.h"
#include "katana/URI.h"

namespace katana {

class KATANA_EXPORT StorageBackedArrowArray {
public:
  StorageBackedArrowArray(const StorageBackedArrowArray& no_copy) = delete;
  StorageBackedArrowArray& operator=(const StorageBackedArrowArray& no_copy) =
      delete;
  StorageBackedArrowArray(StorageBackedArrowArray&& no_move) = delete;
  StorageBackedArrowArray& operator=(StorageBackedArrowArray&& no_move) =
      delete;
  ~StorageBackedArrowArray();

  /// construct and populate with the provided initial array
  static Result<std::shared_ptr<StorageBackedArrowArray>> Make(
      const URI& storage_location,
      const std::shared_ptr<LazyArrowArray>& array);

  /// construct and populate with the provided initial array
  static Result<std::shared_ptr<StorageBackedArrowArray>> Make(
      const URI& storage_location,
      const std::shared_ptr<arrow::ChunkedArray>& array) {
    return Make(
        storage_location, std::make_shared<LazyArrowArray>(
                              array, storage_location.RandFile("op_part")));
  }

  /// construct and populate with an initial null array of length null_count
  static Result<std::shared_ptr<StorageBackedArrowArray>> Make(
      const URI& storage_location, const std::shared_ptr<arrow::DataType>& type,
      int64_t null_count);

  /// Load the results of a `Persist` call to reconstruct an Array in
  /// memory
  /// \param array_file is the uri of the file containing array data
  static std::future<CopyableResult<std::shared_ptr<StorageBackedArrowArray>>>
  FromStorageAsync(const URI& array_file);

  static Result<std::shared_ptr<StorageBackedArrowArray>> FromStorage(
      const URI& array_file) {
    return KATANA_CHECKED(FromStorageAsync(array_file).get());
  }

  template <typename ArrayType>
  static Result<std::shared_ptr<StorageBackedArrowArray>> Append(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<ArrayType>& to_append) {
    return Append(self, self->MakeLazyWrapper(to_append));
  }

  static Result<std::shared_ptr<StorageBackedArrowArray>> Append(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<LazyArrowArray>& to_append);

  static Result<std::shared_ptr<StorageBackedArrowArray>> Append(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<StorageBackedArrowArray>& to_append);

  static Result<std::shared_ptr<StorageBackedArrowArray>> AppendNulls(
      const std::shared_ptr<StorageBackedArrowArray>& self, int64_t null_count);

  template <typename IndexArrayType>
  static Result<std::shared_ptr<StorageBackedArrowArray>> TakeAppend(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<IndexArrayType>& indexes) {
    return TakeAppend(self, self->MakeLazyWrapper(indexes));
  }

  static Result<std::shared_ptr<StorageBackedArrowArray>> TakeAppend(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<LazyArrowArray>& indexes);

  template <typename IndexArrayType, typename SourceArrayType>
  static Result<std::shared_ptr<StorageBackedArrowArray>> TakeAppend(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<IndexArrayType>& indexes,
      const std::shared_ptr<SourceArrayType>& source) {
    return TakeAppend(
        self, self->MakeLazyWrapper(indexes), self->MakeLazyWrapper(source));
  }

  template <typename IndexArrayType>
  static Result<std::shared_ptr<StorageBackedArrowArray>> TakeAppend(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<IndexArrayType>& indexes,
      const std::shared_ptr<StorageBackedArrowArray>& source) {
    return TakeAppend(self, self->MakeLazyWrapper(indexes), source);
  }

  static Result<std::shared_ptr<StorageBackedArrowArray>> TakeAppend(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<LazyArrowArray>& indexes,
      const std::shared_ptr<StorageBackedArrowArray>& source);

  static Result<std::shared_ptr<StorageBackedArrowArray>> TakeAppend(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<LazyArrowArray>& indexes,
      const std::shared_ptr<LazyArrowArray>& source);

  /// Get the underlying arrow array, digesting pending operations
  /// if necessary. If de-chunk  is set to true, make sure the result
  /// only has one chunk
  Result<std::shared_ptr<arrow::ChunkedArray>> GetArray(bool de_chunk = true);

  /// Get a slice of the underlying arrow array, digesting only as many pending
  /// operations as necessary. If de-chunk  is set to true, make sure the result
  /// only has one chunk
  Result<std::shared_ptr<arrow::ChunkedArray>> GetSlice(
      int64_t offset, int64_t length, bool de_chunk = true);

  Result<void> Unload(WriteGroup* wg = nullptr);

  Result<URI> Persist(WriteGroup* wg = nullptr);

  int64_t length() const { return length_; }
  int64_t materialized_length() const {
    return materialized_ ? materialized_->length() : -1;
  }
  const std::shared_ptr<arrow::DataType>& type() const { return type_; }
  bool IsMaterialized(int64_t desired_length = -1) const {
    return desired_length >= 0 ? materialized_length() >= desired_length
                               : materialized_length() == length();
  }

  class DeferredOperation;

private:
  StorageBackedArrowArray(
      URI storage_location, std::shared_ptr<arrow::DataType> type,
      std::shared_ptr<StorageBackedArrowArray> prefix);

  Result<void> SetOps(std::list<std::unique_ptr<DeferredOperation>> ops);

  template <typename DeferredOpType, typename... Args>
  Result<std::shared_ptr<StorageBackedArrowArray>> static MakeWithOp(
      const URI& storage_location, const std::shared_ptr<arrow::DataType>& type,
      Args&&... args);

  template <typename DeferredOpType, typename... Args>
  static Result<std::shared_ptr<katana::StorageBackedArrowArray>> AppendOp(
      const std::shared_ptr<StorageBackedArrowArray>& self, Args&&... args);

  template <typename DeferredOpType, typename... Args>
  static Result<std::shared_ptr<katana::StorageBackedArrowArray>> MakeWithOp(
      const URI& storage_location, const std::shared_ptr<arrow::DataType>& type,
      const std::shared_ptr<StorageBackedArrowArray>& append_to,
      Args&&... args);

  template <typename DeferredOpType, typename... Args>
  static Result<std::shared_ptr<StorageBackedArrowArray>> MakeCommon(
      const URI& storage_location, const std::shared_ptr<arrow::DataType>& type,
      const std::shared_ptr<StorageBackedArrowArray>& prefix, Args&&... args);

  template <typename ArrayPtr>
  std::shared_ptr<LazyArrowArray> MakeLazyWrapper(
      const std::shared_ptr<ArrayPtr>& arr) const {
    return std::make_shared<LazyArrowArray>(
        arr, storage_location_.RandFile("op_part"));
  }

  std::shared_ptr<LazyArrowArray> MakeLazyWrapper(
      const std::shared_ptr<LazyArrowArray>& arr) {
    return arr;
  }

  // apply pending ops until the length of the materialized array is at
  // least `max_bound` - if `max_bound` is negative all pending ops will be
  // applied
  Result<void> ApplyOp(int64_t max_bound = -1);

  // append an array to the one we're holding onto; don't change length though
  // because we prebg-computed the length as we were adding ops
  Result<void> AppendToMaterialized(
      const std::shared_ptr<arrow::ChunkedArray>& to_append);
  Result<void> AppendToMaterialized(
      const std::shared_ptr<arrow::Array>& to_append);

  Result<void> FillOpEntries(
      const URI& prefix, flatbuffers::FlatBufferBuilder* builder,
      std::vector<flatbuffers::Offset<void>>* entries,
      std::vector<uint8_t>* types, WriteGroup* wg);

  URI storage_location_;
  std::shared_ptr<arrow::DataType> type_;
  std::list<std::unique_ptr<DeferredOperation>> ops_;
  std::shared_ptr<StorageBackedArrowArray> prefix_;
  int64_t length_;

  std::shared_ptr<arrow::ChunkedArray> materialized_;
};

inline Result<std::shared_ptr<StorageBackedArrowArray>>
AppendNulls(
    const std::shared_ptr<StorageBackedArrowArray>& self, int64_t null_count) {
  return StorageBackedArrowArray::AppendNulls(self, null_count);
}

/// pretty wrapper to save callers some typing, OtherArrayType
/// can be `arrow::Array` `arrow::ChunkedArray`,
/// `katana::LazyArrowArray`, or `katana::StorageBackedArrowArray`.
template <typename OtherArrayType>
Result<std::shared_ptr<StorageBackedArrowArray>>
Append(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<OtherArrayType>& to_append) {
  return StorageBackedArrowArray::Append(self, to_append);
}

/// pretty wrapper to save callers some typing, IndexesArrayType
/// can be `arrow::Array` `arrow::ChunkedArray`, or
/// `katana::LazyArrowArray`
/// TODO(thunt): there's no reason not to support `katana::StorageBackedArrowArray`.
template <typename IndexesArrayType>
Result<std::shared_ptr<StorageBackedArrowArray>>
TakeAppend(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<IndexesArrayType>& indexes) {
  return StorageBackedArrowArray::TakeAppend(self, indexes);
}

/// pretty wrapper to save callers some typing, OtherArrayType
/// can be `arrow::Array` `arrow::ChunkedArray`,
/// `katana::LazyArrowArray`, or `katana::StorageBackedArrowArray`.
/// IndexesArrayType can be `arrow::Array` `arrow::ChunkedArray`, or
/// `katana::LazyArrowArray`
/// TODO(thunt): there's no reason not to support `katana::StorageBackedArrowArray`.
template <typename OtherArrayType, typename IndexesArrayType>
Result<std::shared_ptr<StorageBackedArrowArray>>
TakeAppend(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<IndexesArrayType>& indexes,
    const std::shared_ptr<OtherArrayType>& source) {
  return StorageBackedArrowArray::TakeAppend(self, indexes, source);
}

}  // namespace katana

#endif

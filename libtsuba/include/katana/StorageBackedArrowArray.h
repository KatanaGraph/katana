#ifndef KATANA_LIBGLUON_KATANA_STORAGEBACKEDARROWARRAY_H_
#define KATANA_LIBGLUON_KATANA_STORAGEBACKEDARROWARRAY_H_

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

// TODO(thunt): move this to tsuba

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
    return Make(storage_location, MakeLazyWrapper(storage_location, array));
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

  static Result<std::shared_ptr<StorageBackedArrowArray>> Append(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<StorageBackedArrowArray>& to_append);

  static Result<std::shared_ptr<StorageBackedArrowArray>> Append(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<LazyArrowArray>& to_append);

  static Result<std::shared_ptr<StorageBackedArrowArray>> Append(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<arrow::ChunkedArray>& to_append) {
    return Append(self, MakeLazyWrapper(self->storage_location_, to_append));
  }

  static Result<std::shared_ptr<StorageBackedArrowArray>> AppendNulls(
      const std::shared_ptr<StorageBackedArrowArray>& self, int64_t null_count);

  static Result<std::shared_ptr<StorageBackedArrowArray>> TakeAppend(
      const std::shared_ptr<StorageBackedArrowArray>& self,
      const std::shared_ptr<arrow::Array>& indexes);

  /// Get the underlying arrow array, digesting pending operations
  /// if necessary. If de-chunk  is set to true, make sure the result
  /// only has one chunk
  Result<std::shared_ptr<arrow::ChunkedArray>> GetArray(bool de_chunk = true);

  Result<void> Unload(WriteGroup* wg = nullptr);

  Result<URI> Persist(WriteGroup* wg = nullptr);

  int64_t length() const { return length_; }
  const std::shared_ptr<arrow::DataType>& type() const { return type_; }
  bool IsMaterialized() const { return bool{materialized_}; }

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
    return MakeLazyWrapper(storage_location_, arr);
  }

  static std::shared_ptr<LazyArrowArray> MakeLazyWrapper(
      const URI& storage_location,
      const std::shared_ptr<arrow::ChunkedArray>& arr) {
    return std::make_shared<LazyArrowArray>(
        arr, storage_location.RandFile("op_part"));
  }

  static std::shared_ptr<LazyArrowArray> MakeLazyWrapper(
      const URI& storage_location, const std::shared_ptr<arrow::Array>& arr) {
    return MakeLazyWrapper(
        storage_location,
        std::make_shared<arrow::ChunkedArray>(std::vector{arr}, arr->type()));
  }

  // apply pending ops until the length of the materialized array is at
  // least `max_bound`
  Result<void> ApplyOp();

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

// pretty wrappers to save callers some typing

inline Result<std::shared_ptr<StorageBackedArrowArray>>
Append(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<StorageBackedArrowArray>& to_append) {
  return StorageBackedArrowArray::Append(self, to_append);
}

inline Result<std::shared_ptr<StorageBackedArrowArray>>
Append(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<LazyArrowArray>& to_append) {
  return StorageBackedArrowArray::Append(self, to_append);
}

inline Result<std::shared_ptr<StorageBackedArrowArray>>
Append(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<arrow::ChunkedArray>& to_append) {
  return StorageBackedArrowArray::Append(self, to_append);
}

inline Result<std::shared_ptr<StorageBackedArrowArray>>
AppendNulls(
    const std::shared_ptr<StorageBackedArrowArray>& self, int64_t null_count) {
  return StorageBackedArrowArray::AppendNulls(self, null_count);
}

inline Result<std::shared_ptr<StorageBackedArrowArray>>
TakeAppend(
    const std::shared_ptr<StorageBackedArrowArray>& self,
    const std::shared_ptr<arrow::Array>& indexes) {
  return StorageBackedArrowArray::TakeAppend(self, indexes);
}

}  // namespace katana

#endif

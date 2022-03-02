#ifndef KATANA_LIBGLUON_KATANA_LAZYARROWARRAY_H_
#define KATANA_LIBGLUON_KATANA_LAZYARROWARRAY_H_

#include <arrow/api.h>

#include "katana/ParquetReader.h"
#include "katana/ParquetWriter.h"
#include "katana/Result.h"
#include "katana/URI.h"

namespace katana {

class LazyArrowArray {
public:
  LazyArrowArray(const LazyArrowArray& no_copy) = delete;
  LazyArrowArray& operator=(const LazyArrowArray& no_copy) = delete;
  LazyArrowArray(LazyArrowArray&& no_move) = delete;
  LazyArrowArray& operator=(LazyArrowArray&& no_move) = delete;

  ~LazyArrowArray() = default;

  LazyArrowArray(std::shared_ptr<arrow::DataType> type, int64_t length, URI uri)
      : array_(nullptr),
        type_(std::move(type)),
        length_(length),
        uri_(std::move(uri)),
        on_disk_(true) {}

  LazyArrowArray(
      std::shared_ptr<arrow::ChunkedArray> array, URI uri, bool on_disk = false)
      : array_(std::move(array)),
        type_(array_->type()),
        length_(array_->length()),
        uri_(std::move(uri)),
        on_disk_(on_disk) {}

  static Result<std::unique_ptr<LazyArrowArray>> Make(URI uri) {
    auto reader = KATANA_CHECKED(katana::ParquetReader::Make());
    auto schema = KATANA_CHECKED(reader->GetSchema(uri));
    if (schema->num_fields() != 1) {
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument,
          "array files must have exactly one column");
    }
    const auto& type = schema->field(0)->type();
    int64_t length = KATANA_CHECKED(reader->NumRows(uri));
    return std::make_unique<LazyArrowArray>(type, length, std::move(uri));
  }

  Result<std::shared_ptr<arrow::ChunkedArray>> Get() {
    if (!array_) {
      auto reader = KATANA_CHECKED(katana::ParquetReader::Make());
      array_ = KATANA_CHECKED(reader->ReadColumn(uri_, 0))->column(0);
    }
    return array_;
  }

  Result<void> Unload(WriteGroup* wg = nullptr) {
    KATANA_CHECKED(Persist(wg));
    array_.reset();
    return ResultSuccess();
  }

  Result<void> Persist(WriteGroup* wg = nullptr) {
    if (!on_disk_) {
      auto writer =
          KATANA_CHECKED(ParquetWriter::Make(array_, uri().BaseName()));
      KATANA_CHECKED(writer->WriteToUri(uri(), wg));
      on_disk_ = true;
    }
    return ResultSuccess();
  }

  int64_t length() const { return length_; }
  const std::shared_ptr<arrow::DataType>& type() const { return type_; }
  const URI& uri() const { return uri_; }
  bool IsOnDisk() const { return on_disk_; }
  bool IsInMemory() const { return bool{array_}; }

private:
  std::shared_ptr<arrow::ChunkedArray> array_;
  std::shared_ptr<arrow::DataType> type_;
  int64_t length_;
  URI uri_;
  bool on_disk_;
};

}  // namespace katana

#endif

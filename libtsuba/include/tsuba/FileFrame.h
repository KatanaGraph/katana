#ifndef GALOIS_LIBTSUBA_TSUBA_FILEFRAME_H_
#define GALOIS_LIBTSUBA_TSUBA_FILEFRAME_H_

#include <cstdint>
#include <string>

#include <parquet/arrow/writer.h>

#include "galois/Logging.h"
#include "galois/Result.h"

namespace tsuba {

class GALOIS_EXPORT FileFrame : public arrow::io::OutputStream {
  std::string path_;
  uint8_t* map_start_;
  uint64_t map_size_;
  uint64_t region_size_;
  uint64_t cursor_;
  bool valid_ = false;
  bool synced_ = false;
  galois::Result<void> GrowBuffer(int64_t accommodate);

public:
  FileFrame() = default;
  FileFrame(const FileFrame&) = delete;
  FileFrame& operator=(const FileFrame&) = delete;

  FileFrame(FileFrame&& other) noexcept
      : path_(other.path_),
        map_start_(other.map_start_),
        map_size_(other.map_size_),
        region_size_(other.region_size_),
        cursor_(other.cursor_),
        valid_(other.valid_),
        synced_(other.synced_) {
    other.valid_ = false;
  }

  FileFrame& operator=(FileFrame&& other) noexcept {
    if (&other != this) {
      if (auto res = Destroy(); !res) {
        GALOIS_LOG_ERROR("Destroy: {}", res.error());
      }
      path_ = other.path_;
      map_start_ = other.map_start_;
      map_size_ = other.map_size_;
      region_size_ = other.region_size_;
      cursor_ = other.cursor_;
      synced_ = other.synced_;
      valid_ = other.valid_;
      other.valid_ = false;
    }
    return *this;
  }

  ~FileFrame() override;

  galois::Result<void> Init(uint64_t reserve_size);
  galois::Result<void> Init() { return Init(1); }
  void Bind(std::string_view filename);

  galois::Result<void> Destroy();

  galois::Result<void> Persist();

  template <typename T>
  galois::Result<T*> ptr() const {
    return reinterpret_cast<T*>(map_start_); /* NOLINT */
  }

  ///// Begin arrow::io::BufferOutputStream methods ///////

  arrow::Status Close() override;
  arrow::Result<int64_t> Tell() const override;
  bool closed() const override;
  arrow::Status Write(const void* data, int64_t nbytes) override;
  arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override;

  ///// End arrow::io::BufferOutputStream methods ///////
};

} /* namespace tsuba */

#endif

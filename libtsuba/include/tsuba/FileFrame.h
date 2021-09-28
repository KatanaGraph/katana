#ifndef KATANA_LIBTSUBA_TSUBA_FILEFRAME_H_
#define KATANA_LIBTSUBA_TSUBA_FILEFRAME_H_

#include <cstdint>
#include <future>
#include <string>

#include <arrow/status.h>
#include <parquet/arrow/writer.h>

#include "katana/Logging.h"
#include "katana/Result.h"

namespace tsuba {

class KATANA_EXPORT FileFrame : public arrow::io::OutputStream {
  std::string path_;
  uint8_t* map_start_;
  uint64_t map_size_;
  uint64_t region_size_;
  uint64_t cursor_;
  bool valid_ = false;
  bool synced_ = false;

  katana::Result<void> GrowBuffer(int64_t accommodate);

  katana::Result<void> MapContguousExtension(uint64_t new_size);

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
        KATANA_LOG_ERROR("Destroy: {}", res.error());
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

  katana::Result<void> Init(uint64_t reserve_size);
  katana::Result<void> Init() { return Init(1); }
  void Bind(std::string_view filename);

  katana::Result<void> Destroy();

  katana::Result<void> Persist();
  std::future<katana::CopyableResult<void>> PersistAsync();

  uint64_t map_size() const { return map_size_; }

  template <typename T>
  katana::Result<T*> ptr() const {
    return reinterpret_cast<T*>(map_start_); /* NOLINT */
  }

  /// only data up to cursor is written out, flavors of Write
  /// automatically track this, so SetCursor is only useful when
  /// treating the FileFrame like a buffer (e.g., using ptr())
  katana::Result<void> SetCursor(uint64_t new_cursor);

  const std::string& path() const { return path_; }

  /// Calculate the number of padding bytes needed to reach nearest byte_boundry
  /// size_t num_bytes : size of bytes
  /// size_t byte_boundary : number of bytes per boundary
  /// ex: if num_bytes = 65, and byte_boundry = 8, return = 7
  static size_t calculate_padding_bytes(
      size_t num_bytes, size_t byte_boundary) {
    size_t padding = 0;
    if (num_bytes >= byte_boundary) {
      padding = ((byte_boundary)-num_bytes % (byte_boundary)) % (byte_boundary);
    } else {
      padding = byte_boundary - num_bytes;
    }
    return padding;
  }

  katana::Result<void> PaddedWrite(
      const void* data, int64_t nbytes, size_t byte_boundry);
  katana::Result<void> PaddedWrite(
      const std::shared_ptr<arrow::Buffer>& data, size_t byte_boundry);

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

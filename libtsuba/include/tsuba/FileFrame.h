#ifndef GALOIS_LIBTSUBA_TSUBA_FILE_FRAME_H_
#define GALOIS_LIBTSUBA_TSUBA_FILE_FRAME_H_

#include <string>
#include <cstdint>

#include <parquet/arrow/writer.h>

namespace tsuba {

class FileFrame : public arrow::io::OutputStream {
  std::string path_;
  uint8_t* map_start_;
  uint64_t map_size_;
  uint64_t region_size_;
  uint64_t cursor_;
  bool valid_  = false;
  bool synced_ = false;
  int GrowBuffer(int64_t accommodate);

public:
  FileFrame()                 = default;
  FileFrame(const FileFrame&) = delete;
  FileFrame& operator=(const FileFrame&) = delete;

  FileFrame(FileFrame&& other) noexcept
      : path_(other.path_), map_start_(other.map_start_),
        map_size_(other.map_size_), region_size_(other.region_size_),
        cursor_(other.cursor_), valid_(other.valid_), synced_(other.synced_) {
    other.valid_ = false;
  }

  FileFrame& operator=(FileFrame&& other) noexcept {
    if (&other != this) {
      path_        = other.path_;
      map_start_   = other.map_start_;
      map_size_    = other.map_size_;
      region_size_ = other.region_size_;
      cursor_      = other.cursor_;
      synced_      = other.synced_;
    }
    return *this;
  }

  ~FileFrame();

  int Init(uint64_t reserve_size);
  inline int Init() { return Init(1); }
  void Bind(const std::string& filename);

  int Destroy();

  int Persist();

  template <typename T>
  const T* ptr() const {
    return reinterpret_cast<T*>(map_start_); /* NOLINT */
  }

  ///// Begin arrow::io::BufferOutputStream methods ///////

  virtual arrow::Status Close();
  virtual arrow::Result<int64_t> Tell() const;
  virtual bool closed() const;
  virtual arrow::Status Write(const void*, int64_t);
  virtual arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data);

  ///// End arrow::io::BufferOutputStream methods ///////
};

} /* namespace tsuba */

#endif

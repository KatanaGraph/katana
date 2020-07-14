#include "tsuba/FileFrame.h"

#include <sys/mman.h>

#include "galois/Logging.h"
#include "galois/Platform.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

namespace tsuba {

FileFrame::~FileFrame() {
  if (auto res = Destroy(); !res) {
    GALOIS_LOG_ERROR("Destroy failed in ~FileFrame");
  }
}

galois::Result<void> FileFrame::Destroy() {
  if (valid_) {
    int err = munmap(map_start_, map_size_);
    valid_  = false;
    if (err) {
      return galois::ResultErrno();
    }
  }
  return galois::ResultSuccess();
}

galois::Result<void> FileFrame::Init(uint64_t reserved_size) {
  size_t size_to_reserve = reserved_size <= 0 ? 1 : reserved_size;
  uint64_t map_size      = tsuba::RoundUpToBlock(size_to_reserve);
  void* ptr = galois::MmapPopulate(nullptr, map_size, PROT_READ | PROT_WRITE,
                                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if (ptr == MAP_FAILED) {
    return galois::ResultErrno();
  }
  if (auto res = Destroy(); !res) {
    GALOIS_LOG_ERROR("Destroy: {}", res.error());
  }
  path_      = "";
  map_size_  = map_size;
  map_start_ = static_cast<uint8_t*>(ptr);
  synced_    = false;
  valid_     = true;
  cursor_    = 0;
  return galois::ResultSuccess();
}

void FileFrame::Bind(const std::string& filename) { path_ = filename; }

galois::Result<void> FileFrame::GrowBuffer(int64_t accomodate) {
  // We need a bigger buffer
  auto new_size = map_size_ * 2;
  while (cursor_ + accomodate > new_size) {
    new_size *= 2;
  }
  void* ptr = galois::MmapPopulate(map_start_ + map_size_, new_size - map_size_,
                                   PROT_READ | PROT_WRITE,
                                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if (ptr != map_start_ + map_size_) {
    if (ptr != MAP_FAILED) {
      // Mapping succeeded, but not where we wanted it
      int err = munmap(ptr, new_size - map_size_);
      if (err) {
        return galois::ResultErrno();
      }
    } else {
      return galois::ResultErrno();
    }
    // Just allocate a brand new buffer :(
    ptr = nullptr;
    ptr = galois::MmapPopulate(nullptr, new_size, PROT_READ | PROT_WRITE,
                               MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (ptr == MAP_FAILED) {
      return galois::ResultErrno();
    }
    memmove(ptr, map_start_, cursor_);
    int err = munmap(map_start_, map_size_);
    if (err) {
      return galois::ResultErrno();
    }
    map_start_ = static_cast<uint8_t*>(ptr);
  }
  map_size_ = new_size;
  return galois::ResultSuccess();
}

galois::Result<void> FileFrame::Persist() {
  if (!valid_) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  if (!path_.length()) {
    GALOIS_LOG_DEBUG("No path provided to FileFrame");
    return tsuba::ErrorCode::InvalidArgument;
  }
  if (auto res = tsuba::FileStore(path_, map_start_, cursor_); !res) {
    return res.error();
  }
  return galois::ResultSuccess();
}

/////// Begin arrow::io::BufferOutputStream method definitions //////

arrow::Status FileFrame::Close() {
  if (auto res = Destroy(); !res) {
    return arrow::Status::UnknownError("FileFrame::Destroy", res.error());
  }
  return arrow::Status::OK();
}

arrow::Result<int64_t> FileFrame::Tell() const {
  if (!valid_) {
    return -1;
  }
  return cursor_;
}

bool FileFrame::closed() const { return !valid_; }

arrow::Status FileFrame::Write(const void* data, int64_t nbytes) {
  if (!valid_) {
    return arrow::Status(arrow::StatusCode::Invalid, "Invalid FileFrame");
  }
  if (nbytes < 0) {
    return arrow::Status(arrow::StatusCode::Invalid,
                         "Cannot Write negative bytes");
  }
  if (cursor_ + nbytes > map_size_) {
    if (auto res = GrowBuffer(nbytes); !res) {
      return arrow::Status(
          arrow::StatusCode::OutOfMemory,
          "FileFrame could not grow buffer to hold incoming write");
    }
  }
  memcpy(map_start_ + cursor_, data, nbytes);
  cursor_ += nbytes;
  return arrow::Status::OK();
}

arrow::Status FileFrame::Write(const std::shared_ptr<arrow::Buffer>& data) {
  return Write(data->data(), data->size());
}
//////////// End arrow::io::BufferOutputStream method definitions ////////

} /* namespace tsuba */

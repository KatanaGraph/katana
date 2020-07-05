#include "tsuba/FileFrame.h"

#include <sys/mman.h>

#include "galois/Logging.h"
#include "galois/Platform.h"
#include "tsuba/file.h"

namespace tsuba {

FileFrame::~FileFrame() { Destroy(); }

int FileFrame::Destroy() {
  if (valid_) {
    int err = munmap(map_start_, map_size_);
    valid_  = false;
    return err;
  }
  return -1;
}

int FileFrame::Init(uint64_t reserved_size) {
  size_t size_to_reserve = reserved_size <= 0 ? 1 : reserved_size;
  uint64_t map_size      = tsuba::RoundUpToBlock(size_to_reserve);
  void* ptr =
      galois::MmapPopulate(nullptr, map_size, PROT_READ | PROT_WRITE,
                           MAP_ANONYMOUS | MAP_PRIVATE | MAP_32BIT, -1, 0);
  if (ptr == MAP_FAILED) {
    perror("mmap");
    return -1;
  }
  Destroy();
  path_      = "";
  map_size_  = map_size;
  map_start_ = static_cast<uint8_t*>(ptr);
  synced_    = false;
  valid_     = true;
  cursor_    = 0;
  return 0;
}

void FileFrame::Bind(const std::string& filename) { path_ = filename; }

int FileFrame::GrowBuffer(int64_t accomodate) {
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
        perror("munmap");
        return -1;
      }
    } else {
      perror("mmap");
    }
    // Just allocate a brand new buffer :(
    ptr = nullptr;
    ptr = galois::MmapPopulate(nullptr, new_size, PROT_READ | PROT_WRITE,
                               MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (ptr == MAP_FAILED) {
      perror("mmap");
      return -1;
    }
    memmove(ptr, map_start_, cursor_);
    int err = munmap(map_start_, map_size_);
    if (err) {
      perror("munmap");
      return -1;
    }
    map_start_ = static_cast<uint8_t*>(ptr);
  }
  map_size_ = new_size;
  return 0;
}

int FileFrame::Persist() {
  if (!valid_) {
    return -1;
  }
  if (!path_.length()) {
    GALOIS_LOG_DEBUG("No path provided to FileFrame");
    return -1;
  }
  return tsuba::FileStore(path_, map_start_, cursor_);
}

/////// Begin arrow::io::BufferOutputStream method definitions //////

arrow::Status FileFrame::Close() {
  int err = Destroy();
  if (err) {
    return arrow::Status::UnknownError("FileFrame::Destroy", err);
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
    int err = GrowBuffer(nbytes);
    if (err) {
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

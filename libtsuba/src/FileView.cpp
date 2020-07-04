#include "tsuba/FileView.h"

#include <unistd.h>
#include <sys/mman.h>

#include <cstdio>
#include <cassert>
#include <string>

#include "galois/Logging.h"
#include "tsuba/file.h"

namespace tsuba {

FileView::~FileView() { Unbind(); }

int FileView::Unbind() {
  if (valid_) {
    int err = 0;
    if (map_start_ != nullptr) {
      err = FileMunmap(map_start_);
    }
    valid_ = false;
    return err;
  }
  return -1;
}

int FileView::Bind(const std::string& filename) {
  StatBuf buf;
  int err = FileStat(filename, &buf);
  if (err) {
    return err;
  }

  return Bind(filename, 0, buf.size);
}

int FileView::Bind(const std::string& filename, uint64_t begin, uint64_t end) {
  assert(begin <= end);
  if (end - begin >
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    GALOIS_LOG_ERROR("FileView region must be indexable by int64_t, recieved "
                     "region of size {:d}",
                     end - begin);
    return -1;
  }

  uint64_t file_off   = RoundDownToBlock(begin);
  uint64_t map_size   = RoundUpToBlock(end - file_off);
  int64_t region_size = end - begin;
  uint8_t* ptr        = nullptr;

  // size of 0 is an invalid thing to pass to Mmap, but we want to support
  // 0 length FileViews (useful for new files)
  if (map_size != 0) {
    ptr = FileMmap(filename, file_off, map_size);
    if (!ptr) {
      return -1;
    }
  }

  Unbind();
  map_size_     = map_size;
  region_size_  = region_size;
  map_start_    = ptr;
  region_start_ = ptr + (begin & kBlockOffsetMask); /* NOLINT */
  valid_        = true;
  cursor_       = 0;
  return 0;
}

////////////// Begin arrow::io::RandomAccessFile method definitions ////////////

arrow::Status FileView::Close() {
  int err = Unbind();
  if (err) {
    return arrow::Status::UnknownError("Unbind", err);
  }
  return arrow::Status::OK();
}

arrow::Result<int64_t> FileView::Tell() const {
  if (!valid_) {
    return -1;
  }
  return cursor_;
}

bool FileView::closed() const { return !valid_; }

arrow::Status FileView::Seek(int64_t seek_to) {
  if (!valid_) {
    return arrow::Status(arrow::StatusCode::Invalid,
                         "Cannot Seek in unbound FileView");
  }
  if (seek_to > region_size_ || seek_to < 0) {
    const std::string message =
        std::string("Cannot Seek to ") + std::to_string(seek_to) +
        " in region of size " + std::to_string(region_size_);
    return arrow::Status(arrow::StatusCode::Invalid, message);
  }
  cursor_ = seek_to;
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> FileView::Read(int64_t nbytes) {
  if (nbytes <= 0 || !valid_) {
    return std::make_shared<arrow::Buffer>(region_start_, 0);
  }
  int64_t nbytes_internal = nbytes;
  if (cursor_ + nbytes > region_size_) {
    nbytes_internal = region_size_ - cursor_;
  }
  auto ret =
      std::make_shared<arrow::Buffer>(region_start_ + cursor_, nbytes_internal);
  cursor_ += nbytes_internal;
  return ret;
}

arrow::Result<int64_t> FileView::Read(int64_t nbytes, void* out) {
  if (nbytes <= 0) {
    return nbytes;
  }
  if (!valid_) {
    return -1;
  }
  int64_t nbytes_internal = nbytes;
  if (cursor_ + nbytes > region_size_) {
    nbytes_internal = region_size_ - cursor_;
  }
  std::memcpy(out, region_start_ + cursor_, nbytes_internal);
  cursor_ += nbytes_internal;
  return nbytes_internal;
}

arrow::Result<int64_t> FileView::GetSize() { return region_size_; }

///// End arrow::io::RandomAccessFile method definitions /////////

} // namespace tsuba

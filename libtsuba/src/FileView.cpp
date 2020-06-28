#include "tsuba/FileView.h"

#include <unistd.h>
#include <sys/mman.h>

#include <cstdio>
#include <cassert>
#include <string>
#include <iostream>

#include "galois/Logging.h"
#include "tsuba/tsuba.h"

namespace tsuba {

FileView::~FileView() { Unbind(); }

int FileView::Unbind() {
  if (valid_) {
    int err = 0;
    if (map_start_ != nullptr) {
      err = tsuba::Munmap(map_start_);
    }
    valid_ = false;
    return err;
  }
  return 0;
}

int FileView::Bind(const std::string& filename) {
  tsuba::StatBuf buf;
  int err = tsuba::Stat(filename, &buf);
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

  uint64_t file_off   = tsuba::RoundDownToBlock(begin);
  uint64_t map_size   = tsuba::RoundUpToBlock(end - file_off);
  int64_t region_size = end - begin;
  uint8_t* ptr        = nullptr;

  // size of 0 is an invalid thing to pass to Mmap, but we want to support
  // 0 length FileViews (useful for new files)
  if (map_size != 0) {
    ptr = Mmap(filename, file_off, map_size);
    if (!ptr) {
      return -1;
    }
  }

  Unbind();
  map_size_     = map_size;
  region_size_  = region_size;
  map_start_    = ptr;
  region_start_ = ptr + (begin & tsuba::kBlockOffsetMask); /* NOLINT */
  valid_        = true;
  cursor_       = 0;
  return 0;
}

////////////// Begin arrow::io::RandomAccessFile method definitions ////////////

// TODO: get error status from tsuba::Munmap via Unbind to return useful status?
arrow::Status FileView::Close() {
  int err = Unbind();
  if (err) {
    return arrow::Status::UnknownError("Unbind", err);
  }
  return arrow::Status::OK();
}

arrow::Result<long int> FileView::Tell() const { return cursor_; }

bool FileView::closed() const { return valid_; }

arrow::Status FileView::Seek(int64_t seek_to) {
  if (seek_to > region_size_) {
    return arrow::Status(arrow::StatusCode::Invalid,
                         "Cannot Seek past end of file");
  }
  cursor_ = seek_to;
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> FileView::Read(int64_t nbytes) {
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
  int64_t nbytes_internal = nbytes;
  if (cursor_ + nbytes > region_size_) {
    nbytes_internal = region_size_ - cursor_;
  }
  std::memcpy(out, region_start_ + cursor_, nbytes_internal);
  cursor_ += nbytes_internal;
  return nbytes_internal;
}

arrow::Result<int64_t> FileView::GetSize() { return region_size_; }

/////////////// End arrow::io::RandomAccessFile method definitions
////////////////

} // namespace tsuba

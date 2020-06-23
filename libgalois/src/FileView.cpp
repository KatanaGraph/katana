#include <cstdio>
#include <cassert>
#include <string>
#include <iostream>

#include <unistd.h>
#include <sys/mman.h>

#include "galois/FileView.h"
#include "galois/Logging.h"
#include "tsuba/tsuba.h"

namespace galois {

FileView::~FileView() { Unbind(); }

void FileView::Unbind() {
  if (valid_) {
    tsuba::Munmap(map_start_);
    valid_ = false;
  }
}

int FileView::Bind(const std::string& filename) {
  tsuba::StatBuf buf;
  int err = tsuba::Stat(filename, &buf);
  if (err) {
    return err;
  }

  return Bind(filename, 0, tsuba::RoundUpToBlock(buf.size));
}

int FileView::Bind(const std::string& filename, uint64_t begin, uint64_t end) {
  assert(begin < end);
  if (end - begin > std::numeric_limits<int64_t>::max()) {
    GALOIS_LOG_ERROR("FileView region must be indexable by int64_t, recieved region of size {:d}", end - begin);
    return -1;
  }

  uint64_t file_off    = tsuba::RoundDownToBlock(begin);
  uint64_t map_size    = tsuba::RoundUpToBlock(end - file_off);
  int64_t region_size = end - begin;

  uint8_t* ptr = tsuba::Mmap(filename, file_off, map_size);
  if (!ptr) {
    return -1;
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

//TODO: get error status from TsubUnmap via Unbind to return useful status?
arrow::Status FileView::Close() {
  Unbind();
  return arrow::Status(arrow::StatusCode::OK);
}

arrow::Result<long int> FileView::Tell() const {
  return cursor_;
}

bool FileView::closed() const {
  return valid_;
}

arrow::Status FileView::Seek(int64_t seek_to) {
  cursor_ = seek_to;
  return arrow::Status(arrow::StatusCode::OK);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> FileView::Read(int64_t nbytes) {
  std::shared_ptr<arrow::Buffer> ret = std::shared_ptr<arrow::Buffer>(new arrow::Buffer(region_start_+cursor_, nbytes));
  cursor_ += nbytes;
  return ret;
}

arrow::Result<int64_t> FileView::Read(int64_t nbytes, void *out) {
  int64_t nbytes_internal = nbytes;
  if (cursor_+nbytes > region_size_) {
    nbytes_internal = region_size_ - cursor_;
  }
  std::memcpy(out, region_start_+cursor_, nbytes_internal);
  cursor_ += nbytes_internal;
  return nbytes_internal;
}

arrow::Result<int64_t> FileView::GetSize() {
  return region_size_;
}

/////////////// End arrow::io::RandomAccessFile method definitions /////////////

} /* namespace galois */

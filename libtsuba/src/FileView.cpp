#include "tsuba/FileView.h"

#include <sys/mman.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <string>

#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

/*
 * SCB 2020-07-23
 * We have a problem here involving modifying the underlying file. The problem
 * is that if the underlying file is modified after the FileView is opened, all
 * sorts of bad things might happen.
 *
 * One solution is to add a modified time field to FileStatBuf and invalidate
 * the whole memory region whenever we discover the file has changed. This would
 * require us to stat the file on every read. We could also attempt to use file
 * locking to make the underlying file read only. But that seems likely to be a
 * hairy mess that doesn't work properly?
 *
 * This is not a huge issue right now as S3 objects are immutable. And we could
 * maybe solve the problem by internally treating local files as immutable as
 * well. But that also seems a little dicey: we would have to enforce that
 * somehow and also tell users to not modify our files?
 */

namespace tsuba {

FileView::~FileView() {
  if (auto res = Unbind(); !res) {
    KATANA_LOG_ERROR("Unbind: {}", res.error());
  }
}

katana::Result<void>
FileView::Unbind() {
  if (bound_) {
    // Resolve all outstanding reads so they don't write to the memory we are
    // about to unmap
    KATANA_CHECKED(Resolve(0, file_size_));

    if (map_start_ != nullptr && file_size_ > 0) {
      if (int err = munmap(map_start_, file_size_); err) {
        return KATANA_ERROR(katana::ResultErrno(), "unmapping buffer");
      }
    }
    map_start_ = nullptr;
    file_size_ = 0;
    page_shift_ = 0;
    cursor_ = 0;
    mem_start_ = 0;
    filename_ = "";
    filling_ = std::vector<uint64_t>();
    KATANA_LOG_DEBUG_ASSERT(fetches_->empty());

    bound_ = false;
  }
  return katana::ResultSuccess();
}

katana::Result<void>
FileView::Bind(
    std::string_view filename, uint64_t begin, uint64_t end, bool resolve) {
  StatBuf buf;
  filename_ = filename;
  KATANA_CHECKED(FileStat(filename_, &buf));

  uint64_t in_end = std::min<uint64_t>(end, static_cast<uint64_t>(buf.size));
  if (in_end < begin) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "begin is larger than end or the size of the file - begin: {}, "
        "requested end: {}, size of file: {}",
        begin, end, buf.size);
  }

  // SCB 2020-07-23: Given that page_shift_ is treated as a compile-time
  // constant, it seems silly to have it be a member of this class. But I can
  // imagine one day wanting to set it dynamically based on file type, file
  // size, type of backing storage, etc. So make it a class member and set it
  // here.
  page_shift_ = 20; /* 1M */
  void* tmp = nullptr;

  // Map enough virtual memory to hold entire file, but do not populate it
  if (buf.size > 0) {
    tmp =
        mmap(nullptr, buf.size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (tmp == MAP_FAILED) {
      return KATANA_ERROR(
          katana::ResultErrno(), "reserving contiguous range {}", buf.size);
    }
  }

  KATANA_CHECKED(Unbind());

  map_start_ = static_cast<uint8_t*>(tmp);
  mem_start_ = -1;
  filling_.clear();
  filling_.resize(page_number(buf.size) / 64 + 1, 0);
  file_size_ = buf.size;
  fetches_ = std::make_unique<std::vector<FillingRange>>();
  KATANA_CHECKED_CONTEXT(
      Fill(begin, in_end, resolve), "failed to fill, begin: {}, end: {}", begin,
      in_end);

  cursor_ = 0;
  bound_ = true;
  return katana::ResultSuccess();
}

katana::Result<void>
FileView::Fill(uint64_t begin, uint64_t end, bool resolve) {
  uint64_t in_end = std::min<uint64_t>(end, file_size_);
  uint64_t in_begin = std::min<uint64_t>(begin, in_end);
  uint64_t first_page = 0;
  uint64_t last_page = 0;
  bool found_empty = false;

  // We would check !bound_ but we want to call this in Bind before we have
  // set bound_. fetches_ should be default constructed to
  // nullptr.
  if (!fetches_) {
    return KATANA_ERROR(ErrorCode::InvalidArgument, "not bound");
  }
  // Gracefully handle the fill zero case here to simplify Bind
  if (in_end != in_begin) {
    if (auto opt =
            MustFill(&filling_[0], page_number(in_begin), page_number(in_end));
        opt.has_value()) {
      std::tie(first_page, last_page) = opt.value();
      found_empty = true;
    }

    uint64_t file_off = first_page * (1UL << page_shift_);
    uint64_t map_size = std::min(
        (last_page + 1) * (1UL << page_shift_) - file_off,
        file_size_ - file_off);
    if (found_empty) {
      // Get physical pages for the region we are about to write
      int err =
          mprotect(map_start_ + file_off, map_size, PROT_READ | PROT_WRITE);
      if (err == -1) {
        return KATANA_ERROR(katana::ResultErrno(), "mprotecting buffer");
      }

      auto peek_fut =
          FileGetAsync(filename_, map_start_ + file_off, file_off, map_size);
      KATANA_LOG_ASSERT(peek_fut.valid());
      FillingRange fetch = {first_page, last_page, std::move(peek_fut)};
      fetches_->push_back(std::move(fetch));
      KATANA_CHECKED(MarkFilled(&filling_[0], first_page, last_page));
      if (resolve) {
        KATANA_CHECKED(Resolve(file_off, map_size));
      }
      int64_t signed_begin = static_cast<int64_t>(in_begin);
      if (mem_start_ < 0 || signed_begin < mem_start_) {
        mem_start_ = signed_begin;
      }
    }
  }
  return katana::ResultSuccess();
}

bool
FileView::Equals(const FileView& other) const {
  if (!bound_ || !other.bound_) {
    return false;
  }
  if (size() != other.size()) {
    return false;
  }
  // Consider two FileViews that refer to the same file to be equal,
  // regardless of which portions of the file actually appear in memory.
  if (filename_ != other.filename_) {
    return false;
  }
  return true;
}

////// Begin arrow::io::RandomAccessFile method definitions ////////

arrow::Status
FileView::Close() {
  if (auto res = Unbind(); !res) {
    return arrow::Status::UnknownError("Unbind", res.error());
  }
  return arrow::Status::OK();
}

arrow::Result<int64_t>
FileView::Tell() const {
  if (!bound_) {
    return arrow::Status(
        arrow::StatusCode::Invalid, "Unbound FileView has no cursor position");
  }
  return cursor_;
}

bool
FileView::closed() const {
  return !bound_;
}

arrow::Status
FileView::Seek(int64_t seek_to) {
  if (!bound_) {
    return arrow::Status(
        arrow::StatusCode::Invalid, "Cannot Seek in unbound FileView");
  }
  if (seek_to > file_size_ || seek_to < 0) {
    const std::string message = std::string("Cannot Seek to ") +
                                std::to_string(seek_to) + " in file of size " +
                                std::to_string(file_size_);
    return arrow::Status(arrow::StatusCode::Invalid, message);
  }
  cursor_ = seek_to;
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
FileView::Read(int64_t nbytes) {
  if (!bound_) {
    return arrow::Status(arrow::StatusCode::Invalid, "Unbound FileView");
  }

  // sanitize inputs
  if (nbytes <= 0 || !map_start_) {
    return std::make_shared<arrow::Buffer>(map_start_, 0);
  }
  int64_t nbytes_internal = nbytes;
  if (cursor_ + nbytes > file_size_) {
    nbytes_internal = file_size_ - cursor_;
  }
  // fetch data from storage if necessary
  if (auto res = Fill(cursor_, cursor_ + nbytes_internal, true); !res) {
    return arrow::Status(arrow::StatusCode::IOError, "FileView::Fill");
  }
  // resolve outstanding relevant fetches
  if (auto res = Resolve(cursor_, nbytes_internal); !res) {
    // TODO (scober): Include res.error() as part of arrow Status
    return arrow::Status(
        arrow::StatusCode::IOError, "Resolving asynchronous reads");
  }
  // prefetch
  if (auto res = PreFetch(cursor_, nbytes_internal); !res) {
    // TODO (scober): Include res.error() as part of arrow Status
    return arrow::Status(arrow::StatusCode::IOError, "prefetching");
  }
  // and return the requested data
  auto ret =
      std::make_shared<arrow::Buffer>(map_start_ + cursor_, nbytes_internal);
  cursor_ += nbytes_internal;
  return ret;
}

arrow::Result<int64_t>
FileView::Read(int64_t nbytes, void* out) {
  // sanitize inputs
  if (nbytes <= 0) {
    return nbytes;
  }
  if (!bound_) {
    return arrow::Status(arrow::StatusCode::Invalid, "Unbound FileView");
  }
  int64_t nbytes_internal = nbytes;
  if (cursor_ + nbytes > file_size_) {
    nbytes_internal = file_size_ - cursor_;
  }
  // fetch data from storage if necessary
  if (auto res = Fill(cursor_, cursor_ + nbytes_internal, true); !res) {
    return arrow::Status(arrow::StatusCode::IOError, "FileView::Fill");
  }
  // resolve outstanding relevant fetches
  if (auto res = Resolve(cursor_, nbytes_internal); !res) {
    // TODO (scober): Include res.error() as part of arrow Status
    return arrow::Status(
        arrow::StatusCode::IOError, "Resolving asynchronous reads");
  }
  // prefetch
  if (auto res = PreFetch(cursor_, nbytes_internal); !res) {
    // TODO (scober): Include res.error() as part of arrow Status
    return arrow::Status(arrow::StatusCode::IOError, "prefetching");
  }
  // and return the requested data
  std::memcpy(out, map_start_ + cursor_, nbytes_internal);
  cursor_ += nbytes_internal;
  return nbytes_internal;
}

arrow::Result<int64_t>
FileView::GetSize() {
  return size();
}

///// End arrow::io::RandomAccessFile method definitions /////////

uint64_t
FileView::page_number(uint64_t size) {
  return size >> page_shift_;
}

inline uint64_t
FileView::FirstPage(
    uint64_t* bitmap, uint64_t block_num, uint64_t start, uint64_t end) {
  uint64_t working = ~bitmap[block_num];
  uint64_t mask = 1UL << (63 - start);
  uint64_t page_low_bits = start;
  while (!(working & mask) && page_low_bits <= end) {
    mask >>= 1;
    page_low_bits += 1;
  }
  return block_num * 64 + page_low_bits;
}

inline uint64_t
FileView::LastPage(
    uint64_t* bitmap, uint64_t block_num, uint64_t start, uint64_t end) {
  uint64_t working = ~bitmap[block_num];
  uint64_t mask = 1UL << (63 - end);
  uint64_t page_low_bits = end;
  while (!(working & mask) && page_low_bits >= start) {
    mask <<= 1;
    page_low_bits -= 1;
  }
  return block_num * 64 + page_low_bits;
}

std::optional<std::pair<uint64_t, uint64_t>>
FileView::MustFill(uint64_t* bitmap, uint64_t begin, uint64_t end) {
  /*
   * We want to search for 0 bits in the bit array bitmap.
   *
   * For "internal" blocks, we just need to find any 0 bits in a uint64_t
   * and this is relatively straightforward (i.e. A)
   *
   * But the first and last block might be only partially covered by [begin,
   * end] (i.e. the range [60, 70] would cover the last 4 bits of block 0 and
   * the first 7 bits of block 1).
   *
   * So we construct masks at B with ones in the bit positions we want to
   * check. So for the above example,
   * 0000000000000000000000000000000000000000000000000000000000001111
   * and
   * 1111111000000000000000000000000000000000000000000000000000000000
   *
   * Then we AND the mask against the block in question to zero out the bits
   * we don't care about and XOR the mask against what is left to determine if
   * there are zeroes left in the region we care about (the zeroed region will
   * give all zeroes and the meaningful region will produce a 1 only if there
   * is a zero present). This process occurs at C.
   *
   * Finally, we special case the situation where we only want to check part
   * of one block (i.e. [13, 45]). (D) is the same as C except that we have to
   * create and use The Mask in order to check the right region.
   */
  uint64_t begin_mask; /* B */
  if (begin % 64) {
    // This should work even if begin % 64 == 0, but it doesn't?
    begin_mask = (UINT64_C(1) << (64 - begin % 64)) - UINT64_C(1);
  } else {
    begin_mask = ~UINT64_C(0);
  }
  uint64_t end_mask = ~((UINT64_C(1) << (63 - end % 64)) - UINT64_C(1));

  uint64_t begin_block = begin / 64;
  uint64_t end_block = end / 64;

  if (begin_block == end_block) { /* D */
    uint64_t jim_carrey = begin_mask & end_mask;
    if ((bitmap[begin_block] & jim_carrey) ^ jim_carrey) {
      return std::make_pair(
          FirstPage(bitmap, begin_block, begin % 64, end % 64),
          LastPage(bitmap, begin_block, begin % 64, end % 64));
    }
    return std::nullopt;
  }

  /* C */
  uint64_t begin_block_zeroes = (bitmap[begin_block] & begin_mask) ^ begin_mask;
  uint64_t end_block_zeroes = (bitmap[end_block] & end_mask) ^ end_mask;

  bool found_first = begin_block_zeroes;
  bool found_last = end_block_zeroes;
  uint64_t first_page =
      found_first ? FirstPage(bitmap, begin_block, begin % 64, 63) : 0;
  uint64_t last_page =
      found_last ? LastPage(bitmap, end_block, 0, end % 64) : 0;

  if (!found_first) {
    // search forward for first missing page, skip begin block
    for (uint64_t i = begin_block + 1; i < end_block && !found_first; ++i) {
      if (~bitmap[i]) { /* A */
        first_page = FirstPage(bitmap, i, 0, 63);
        found_first = true;
      }
    }

    if (!found_first && end_block_zeroes) {
      first_page = FirstPage(bitmap, end_block, 0, end % 64);
      found_first = true;
    }
  }

  // If we were unsuccessful searching forward we are unlikely to succeed
  // searching backward
  if (found_first && !found_last) {
    // search backward for last page, skip end_block
    for (uint64_t i = end_block - 1; i > begin_block && !found_last; ++i) {
      if (~bitmap[i]) {
        last_page = LastPage(bitmap, i, 0, 63);
        found_last = true;
      }
    }

    if (!found_last && begin_block_zeroes) {
      last_page = LastPage(bitmap, begin_block, begin % 64, 63);
      found_last = true;
    }
  }

  if (found_first) {
    KATANA_LOG_DEBUG_ASSERT(found_last);
    return std::make_pair(first_page, last_page);
  } else {
    return std::nullopt;
  }
}

katana::Result<void>
FileView::MarkFilled(uint64_t* bitmap, uint64_t begin, uint64_t end) {
  uint64_t begin_mask;
  if (begin % 64) {
    begin_mask = (UINT64_C(1) << (64 - begin % 64)) - UINT64_C(1);
  } else {
    begin_mask = ~UINT64_C(0);
  }
  uint64_t end_mask = ~((UINT64_C(1) << (63 - end % 64)) - UINT64_C(1));

  uint64_t begin_block = begin / 64;
  uint64_t end_block = end / 64;

  if (begin_block == end_block) {
    bitmap[begin_block] |= (begin_mask & end_mask);
  } else {
    bitmap[begin_block] |= begin_mask;
    for (uint64_t i = begin_block + 1; i < end_block; ++i) {
      bitmap[i] |= (uint64_t)(-1);
    }
    bitmap[end_block] |= end_mask;
  }

  return katana::ResultSuccess();
}

katana::Result<void>
FileView::Resolve(int64_t start, int64_t size) {
  // This loop could do less work by sorting the vector or storing an
  // interval tree, but that seems like overkill unless this becomes a
  // bottleneck
  for (auto it = fetches_->begin(); it != fetches_->end();) {
    auto fetch = it;
    if (fetch->first_page <= page_number(start + size) ||
        fetch->last_page >= page_number(start)) {
      // Complete the remaining work if there is some
      if (fetch->work.valid()) {
        KATANA_CHECKED(fetch->work.get());
      } else {
        KATANA_LOG_DEBUG("bad future in FileView::Resolve {} {}", start, size);
      }
      it = fetches_->erase(it);
    } else {
      ++it;
    }
  }
  return katana::ResultSuccess();
}

katana::Result<void>
FileView::PreFetch(int64_t start, int64_t size) {
  // Our highly sophisticated prefetching algorithm is to crudely approximate
  // the size of the last read plus 10%. This is largely motivated by parquet
  // files, which consecutively read row groups that are (in theory)
  // approximately the same size.
  int64_t fetch_size = (size / 10) * 11;
  // Make sure we haven't overflown
  KATANA_LOG_DEBUG_ASSERT(fetch_size >= 0);
  uint64_t begin = static_cast<uint64_t>(start + size);
  uint64_t end = static_cast<uint64_t>(start + size + fetch_size);
  KATANA_CHECKED(Fill(begin, end, false));
  return katana::ResultSuccess();
}
}  // namespace tsuba

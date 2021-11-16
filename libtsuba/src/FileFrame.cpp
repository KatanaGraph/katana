#include "tsuba/FileFrame.h"

#include <sys/mman.h>

#include "katana/Logging.h"
#include "katana/Platform.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

namespace tsuba {

FileFrame::~FileFrame() {
  if (auto res = Destroy(); !res) {
    KATANA_LOG_ERROR("Destroy failed in ~FileFrame");
  }
}

katana::Result<void>
FileFrame::Destroy() {
  if (valid_) {
    int err = munmap(map_start_, map_size_);
    valid_ = false;
    if (err) {
      return KATANA_ERROR(katana::ResultErrno(), "unmapping buffer");
    }
  }
  return katana::ResultSuccess();
}

katana::Result<void>
FileFrame::Init(uint64_t reserved_size) {
  size_t size_to_reserve = reserved_size <= 0 ? 1 : reserved_size;
  uint64_t map_size = tsuba::RoundUpToBlock(size_to_reserve);
  void* ptr = katana::MmapPopulate(
      nullptr, map_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE,
      -1, 0);
  if (ptr == MAP_FAILED) {
    return KATANA_ERROR(katana::ResultErrno(), "mapping buffer");
  }
  KATANA_CHECKED(Destroy());

  path_ = "";
  map_size_ = map_size;
  map_start_ = static_cast<uint8_t*>(ptr);
  synced_ = false;
  valid_ = true;
  cursor_ = 0;
  return katana::ResultSuccess();
}

void
FileFrame::Bind(std::string_view filename) {
  path_ = filename;
}

katana::Result<void>
FileFrame::MapContguousExtension(uint64_t new_size) {
  void* ptr = katana::MmapPopulate(
      map_start_ + map_size_, new_size - map_size_, PROT_READ | PROT_WRITE,
      MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if (ptr != map_start_ + map_size_) {
    if (ptr != MAP_FAILED) {
      // Mapping succeeded, but not where we wanted it
      int err = munmap(ptr, new_size - map_size_);
      if (err) {
        return KATANA_ERROR(katana::ResultErrno(), "unmapping buffer");
      }
    } else {
      return KATANA_ERROR(
          katana::ResultErrno(), "mapping new memory to extend buffer");
    }
    // Just allocate a brand new buffer :(
    ptr = nullptr;
    ptr = katana::MmapPopulate(
        nullptr, new_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE,
        -1, 0);
    if (ptr == MAP_FAILED) {
      return KATANA_ERROR(katana::ResultErrno(), "mapping new buffer");
    }
    memcpy(ptr, map_start_, cursor_);
    int err = munmap(map_start_, map_size_);
    if (err) {
      return KATANA_ERROR(katana::ResultErrno(), "unmapping old buffer");
    }
    map_start_ = static_cast<uint8_t*>(ptr);
  }
  map_size_ = new_size;
  return katana::ResultSuccess();
}

katana::Result<void>
FileFrame::GrowBuffer(int64_t accommodate) {
  // We need a bigger buffer
  auto new_size = map_size_ * 2;
  while (cursor_ + accommodate > new_size) {
    new_size *= 2;
  }
  auto res = MapContguousExtension(new_size);
  if (!res && cursor_ + accommodate < new_size) {
    // our power of 2 alloc failed, but there's a chance a smaller ask
    // would work
    res = MapContguousExtension(cursor_ + accommodate);
  }
  return res;
}

katana::Result<void>
FileFrame::Persist() {
  if (!valid_) {
    return KATANA_ERROR(ErrorCode::InvalidArgument, "not bound");
  }
  if (path_.empty()) {
    return KATANA_ERROR(tsuba::ErrorCode::InvalidArgument, "no path provided");
  }
  KATANA_CHECKED(tsuba::FileStore(path_, map_start_, cursor_));

  return katana::ResultSuccess();
}

std::future<katana::CopyableResult<void>>
FileFrame::PersistAsync() {
  if (!valid_) {
    return std::async(
        std::launch::deferred, []() -> katana::CopyableResult<void> {
          return KATANA_ERROR(ErrorCode::InvalidArgument, "not bound");
        });
  }
  if (path_.empty()) {
    return std::async(
        std::launch::deferred, []() -> katana::CopyableResult<void> {
          return KATANA_ERROR(ErrorCode::InvalidArgument, "no path provided");
        });
  }
  return tsuba::FileStoreAsync(path_, map_start_, cursor_);
}

katana::Result<void>
FileFrame::SetCursor(uint64_t new_cursor) {
  if (new_cursor > map_size_) {
    KATANA_CHECKED(GrowBuffer(new_cursor - map_size_));
  }
  cursor_ = new_cursor;
  return katana::ResultSuccess();
}

/////// Begin arrow::io::BufferOutputStream method definitions //////

arrow::Status
FileFrame::Close() {
  if (auto res = Destroy(); !res) {
    return arrow::Status::UnknownError("FileFrame::Destroy", res.error());
  }
  return arrow::Status::OK();
}

arrow::Result<int64_t>
FileFrame::Tell() const {
  if (!valid_) {
    return -1;
  }
  return cursor_;
}

bool
FileFrame::closed() const {
  return !valid_;
}

arrow::Status
FileFrame::Write(const void* data, int64_t nbytes) {
  if (!valid_) {
    return arrow::Status(arrow::StatusCode::Invalid, "Invalid FileFrame");
  }
  if (nbytes < 0) {
    return arrow::Status(
        arrow::StatusCode::Invalid, "Cannot Write negative bytes");
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

arrow::Status
FileFrame::Write(const std::shared_ptr<arrow::Buffer>& data) {
  return Write(data->data(), data->size());
}
//////////// End arrow::io::BufferOutputStream method definitions ////////

katana::Result<void>
FileFrame::PaddedWrite(
    const std::shared_ptr<arrow::Buffer>& data, size_t byte_boundry) {
  return PaddedWrite(data->data(), data->size(), byte_boundry);
}

katana::Result<void>
FileFrame::PaddedWrite(const void* data, int64_t nbytes, size_t byte_boundry) {
  arrow::Status aro_sts = Write(data, nbytes);
  if (!aro_sts.ok()) {
    return tsuba::ArrowToTsuba(aro_sts.code());
  }

  size_t num_padding_bytes = calculate_padding_bytes(nbytes, byte_boundry);
  KATANA_LOG_DEBUG(
      "adding {} bytes of padding. nbytes = {}, byte_boundry = {}",
      num_padding_bytes, nbytes, byte_boundry);
  if (num_padding_bytes > 0) {
    uint8_t padding[num_padding_bytes];
    aro_sts = Write(&padding, num_padding_bytes * sizeof(uint8_t));
    if (!aro_sts.ok()) {
      return tsuba::ArrowToTsuba(aro_sts.code());
    }
  }

  return katana::ResultSuccess();
}

} /* namespace tsuba */

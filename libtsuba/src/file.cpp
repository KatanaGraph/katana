//
// @file libtsuba/srce/file.cpp
//
// Contains the unstructured entry points for interfacing with the tsuba storage
// server
//
#include "tsuba/file.h"

#include <mutex>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include <cassert>
#include <boost/filesystem.hpp>

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "galois/Platform.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"

#include "s3.h"
#include "tsuba_internal.h"

namespace fs = boost::filesystem;

namespace {

template <typename T>
T* MmapCast(size_t size, int prot, int flags, int fd, off_t off) {
  void* ret = galois::MmapPopulate(nullptr, size, prot, flags, fd, off);
  if (ret == MAP_FAILED) {
    perror("mmap");
    return nullptr;
  }
  return reinterpret_cast<T*>(ret); /* NOLINT cast needed for mmap interface */
}

uint8_t* MmapLocalFile(const std::string& filename, uint64_t begin,
                       uint64_t size) {
  int fd = open(filename.c_str(), O_RDONLY, 0);
  if (fd < 0) {
    return nullptr;
  }
  auto* ret = MmapCast<uint8_t>(size, PROT_READ, MAP_SHARED, fd, begin);
  close(fd);
  return ret;
}

int DoReadS3Part(const std::string& uri, uint8_t* buf, uint64_t begin,
                 uint64_t size) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3DownloadRange(bucket, object, begin, size, buf);
}
int DoS3Exists(const std::string& uri) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3Exists(bucket, object);
}

int DoWriteS3(const std::string& uri, const uint8_t* buf, uint64_t size) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3UploadOverwrite(bucket, object, buf, size);
}

int DoWriteS3Sync(const std::string& uri, const uint8_t* buf, uint64_t size) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3PutSingleSync(bucket, object, buf, size);
}

int DoWriteS3Async(const std::string& uri, const uint8_t* buf, uint64_t size) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3PutSingleAsync(bucket, object, buf, size);
}

int DoWriteS3AsyncFinish(const std::string& uri) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3PutSingleAsyncFinish(bucket, object);
}

int DoWriteS3MultiAsync1(const std::string& uri, const uint8_t* buf,
                         uint64_t size) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3PutMultiAsync1(bucket, object, buf, size);
}

int DoWriteS3MultiAsync2(const std::string& uri) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3PutMultiAsync2(bucket, object);
}

int DoWriteS3MultiAsync3(const std::string& uri) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3PutMultiAsync3(bucket, object);
}

int DoWriteS3MultiAsyncFinish(const std::string& uri) {
  auto [bucket, object] = tsuba::S3SplitUri(uri);
  return tsuba::S3PutMultiAsyncFinish(bucket, object);
}

uint8_t* AllocAndReadS3(const std::string& uri, uint64_t begin, uint64_t size) {
  auto* ret = MmapCast<uint8_t>(size, PROT_READ | PROT_WRITE,
                                MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if (ret == nullptr) {
    std::cerr << "alloc for s3 read failed" << std::endl;
    return nullptr;
  }
  if (DoReadS3Part(uri, ret, begin, size)) {
    munmap(ret, size);
    ret = nullptr;
    std::cerr << "do_read_s3_part failed" << std::endl;
  }
  return ret;
}

class MappingDesc {
  uint8_t* ptr_{nullptr};
  size_t size_{0};
  bool valid_ = false;

public:
  MappingDesc(const MappingDesc&) = delete;
  MappingDesc& operator=(const MappingDesc&) = delete;
  MappingDesc()                              = default;
  MappingDesc(MappingDesc&& other) noexcept
      : ptr_(other.ptr_), size_(other.size_), valid_(other.valid_) {
    other.valid_ = false;
  }
  MappingDesc& operator=(MappingDesc&& other) noexcept {
    if (&other != this) {
      if (valid_) {
        munmap(ptr_, size_);
      }
      ptr_         = other.ptr_;
      size_        = other.size_;
      valid_       = other.valid_;
      other.valid_ = false;
    }
    return *this;
  }
  int Init(const std::string& uri, uint64_t offset, size_t size) {
    assert(!valid_);
    if (tsuba::IsS3URI(uri)) {
      ptr_ = AllocAndReadS3(uri, offset, size);
    } else {
      ptr_ = MmapLocalFile(uri, offset, size);
    }
    if (ptr_ == nullptr) {
      return -1;
    }
    valid_ = true;
    size_  = size;
    return 0;
  }
  ~MappingDesc() {
    if (valid_) {
      munmap(ptr_, size_);
    }
  }
  uint8_t* ptr() { return ptr_; }
};

std::mutex allocated_memory_mutex;
std::unordered_map<uint8_t*, MappingDesc> allocated_memory;

// Return 1: This is an S3 URL
// Otherwise process as file and return 0 for success and -1 for fail
int S3OrWriteFile(const std::string& uri, const uint8_t* data, uint64_t size) {
  if (tsuba::IsS3URI(uri)) {
    return 1;
  }
  std::ofstream ofile(uri);
  if (!ofile.good()) {
    return -1;
  }
  ofile.write(reinterpret_cast<const char*>(data), size); /* NOLINT */
  if (!ofile.good()) {
    return -1;
  }
  return 0;
}

} // namespace

// Recommend rename to FileDownloadOpenUnlink
[[deprecated]] galois::Result<int> tsuba::FileOpen(const std::string& uri) {
  if (!tsuba::IsS3URI(uri)) {
    return ErrorCode::InvalidArgument;
  }
  auto [bucket_name, object_name] = S3SplitUri(uri);
  if (bucket_name.empty() || object_name.empty()) {
    return ErrorCode::OutOfMemory;
  }
  auto result = S3Open(bucket_name, object_name);
  if (!result) {
    return result.error();
  }
  return result.value();
}

galois::Result<void> tsuba::FileCreate(const std::string& filename,
                                       bool overwrite) {
  if (tsuba::IsS3URI(filename)) {
    if (overwrite && DoS3Exists(filename)) {
      return tsuba::ErrorCode::Exists;
    }
    // S3 has atomic puts, so create on write.
    return galois::ResultSuccess();
  } else {
    fs::path m_path{filename};
    if (overwrite && fs::exists(m_path)) {
      return tsuba::ErrorCode::Exists;
    }
    fs::path dir = m_path.parent_path();
    if (boost::system::error_code err; !fs::create_directories(dir, err)) {
      if (err) {
        return err;
      }
    }
    // Creates an empty file
    std::ofstream output(m_path.string());
    return galois::ResultSuccess();
  }
  return tsuba::ErrorCode::NotImplemented;
}

galois::Result<void> tsuba::FileStore(const std::string& uri,
                                      const uint8_t* data, uint64_t size) {
  int ret = S3OrWriteFile(uri, data, size);
  if (ret) {
    ret = DoWriteS3(uri, data, size);
  }
  if (ret) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FileStoreSync(const std::string& uri,
                                          const uint8_t* data, uint64_t size) {
  int ret = S3OrWriteFile(uri, data, size);
  if (ret) {
    ret = DoWriteS3Sync(uri, data, size);
  }
  if (ret) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FileStoreAsync(const std::string& uri,
                                           const uint8_t* data, uint64_t size) {
  int ret = S3OrWriteFile(uri, data, size);
  if (ret) {
    ret = DoWriteS3Async(uri, data, size);
  }
  if (ret) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FileStoreAsyncFinish(const std::string& uri) {
  if (!IsS3URI(uri)) {
    return galois::ResultSuccess();
  }
  if (int err = DoWriteS3AsyncFinish(uri); err) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FileStoreMultiAsync1(const std::string& uri,
                                                 const uint8_t* data,
                                                 uint64_t size) {
  int ret = S3OrWriteFile(uri, data, size);
  if (ret) {
    ret = DoWriteS3MultiAsync1(uri, data, size);
  }
  if (ret) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FileStoreMultiAsync2(const std::string& uri) {
  if (!IsS3URI(uri)) {
    return galois::ResultSuccess();
  }
  if (int err = DoWriteS3MultiAsync2(uri); err) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FileStoreMultiAsync3(const std::string& uri) {
  if (!IsS3URI(uri)) {
    return galois::ResultSuccess();
  }
  if (int err = DoWriteS3MultiAsync3(uri); err) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FileStoreMultiAsyncFinish(const std::string& uri) {
  if (!IsS3URI(uri)) {
    return galois::ResultSuccess();
  }
  if (int err = DoWriteS3MultiAsyncFinish(uri); err) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FilePeek(const std::string& filename,
                                     uint8_t* result_buffer, uint64_t begin,
                                     uint64_t size) {
  if (!IsS3URI(filename)) {
    std::ifstream infile(filename);
    if (!infile.good()) {
      return galois::ResultErrno();
    }
    infile.seekg(begin);
    if (!infile.good()) {
      return galois::ResultErrno();
    }
    infile.read(reinterpret_cast<char*>(result_buffer), size); /* NOLINT */
    if (!infile.good()) {
      return galois::ResultErrno();
    }
    return galois::ResultSuccess();
  }
  if (int err = DoReadS3Part(filename, result_buffer, begin, size); err) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::FileStat(const std::string& filename,
                                     StatBuf* s_buf) {
  if (!IsS3URI(filename)) {
    struct stat local_s_buf;
    if (int ret = stat(filename.c_str(), &local_s_buf); ret) {
      return galois::ResultErrno();
    }
    s_buf->size = local_s_buf.st_size;
    return galois::ResultSuccess();
  }
  auto [bucket_name, object_name] = S3SplitUri(std::string(filename));
  if (int ret = S3GetSize(bucket_name, object_name, &s_buf->size); ret) {
    return ErrorCode::TODO;
  }
  return galois::ResultSuccess();
}

galois::Result<uint8_t*> tsuba::FileMmap(const std::string& filename,
                                         uint64_t begin, uint64_t size) {
  MappingDesc new_mapping;
  if (int err = new_mapping.Init(filename, begin, size); err) {
    return nullptr;
  }
  std::lock_guard<std::mutex> guard(allocated_memory_mutex);
  auto [it, inserted] =
      allocated_memory.emplace(new_mapping.ptr(), std::move(new_mapping));
  if (!inserted) {
    GALOIS_LOG_ERROR("Failed to emplace! (impossible?)");
    return nullptr;
  }
  return it->second.ptr();
}

galois::Result<void> tsuba::FileMunmap(uint8_t* ptr) {
  std::lock_guard<std::mutex> guard(allocated_memory_mutex);
  if (allocated_memory.erase(ptr) != 1) {
    GALOIS_LOG_WARN("passed unknown pointer to tsuba_munmap");
    return tsuba::ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

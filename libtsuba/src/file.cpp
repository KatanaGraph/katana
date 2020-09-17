//
// @file libtsuba/srce/file.cpp
//
// Contains the unstructured entry points for interfacing with the tsuba storage
// server
//
#include "tsuba/file.h"

#include <sys/mman.h>

#include <cassert>
#include <fstream>
#include <iostream>
#include <mutex>
#include <unordered_map>

#include "GlobalState.h"
#include "galois/Logging.h"
#include "galois/Platform.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"

namespace {

template <typename T>
T*
MmapCast(size_t size, int prot, int flags, int fd, off_t off) {
  void* ret = galois::MmapPopulate(nullptr, size, prot, flags, fd, off);
  if (ret == MAP_FAILED) {
    perror("mmap");
    return nullptr;
  }
  return reinterpret_cast<T*>(ret); /* NOLINT cast needed for mmap interface */
}

galois::Result<uint8_t*>
AllocAndRead(const std::string& uri, uint64_t begin, uint64_t size) {
  auto* ret = MmapCast<uint8_t>(
      size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if (ret == nullptr) {
    GALOIS_LOG_DEBUG("alloc for s3 read");
    return galois::ResultErrno();
  }
  if (auto res = tsuba::FS(uri)->GetMultiSync(uri, begin, size, ret); !res) {
    GALOIS_LOG_DEBUG("GetMultisync failed");
    munmap(ret, size);
    return res.error();
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
  MappingDesc() = default;
  MappingDesc(MappingDesc&& other) noexcept
      : ptr_(other.ptr_), size_(other.size_), valid_(other.valid_) {
    other.valid_ = false;
  }
  MappingDesc& operator=(MappingDesc&& other) noexcept {
    if (&other != this) {
      if (valid_) {
        munmap(ptr_, size_);
      }
      ptr_ = other.ptr_;
      size_ = other.size_;
      valid_ = other.valid_;
      other.valid_ = false;
    }
    return *this;
  }
  galois::Result<void> Init(
      const std::string& uri, uint64_t offset, size_t size) {
    assert(!valid_);
    auto ptr_res = AllocAndRead(uri, offset, size);
    if (!ptr_res) {
      return ptr_res.error();
    }
    ptr_ = ptr_res.value();
    valid_ = true;
    size_ = size;
    return galois::ResultSuccess();
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

}  // namespace

galois::Result<void>
tsuba::FileCreate(const std::string& uri, bool overwrite) {
  return FS(uri)->Create(uri, overwrite);
}

galois::Result<void>
tsuba::FileStore(const std::string& uri, const uint8_t* data, uint64_t size) {
  return FS(uri)->PutMultiSync(uri, data, size);
}

galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
tsuba::FileStoreAsync(
    const std::string& uri, const uint8_t* data, uint64_t size) {
  return FS(uri)->PutAsync(uri, data, size);
}

galois::Result<void>
tsuba::FilePeek(
    const std::string& uri, uint8_t* result_buffer, uint64_t begin,
    uint64_t size) {
  return FS(uri)->GetMultiSync(uri, begin, size, result_buffer);
}

galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
tsuba::FilePeekAsync(
    const std::string& uri, uint8_t* result_buffer, uint64_t begin,
    uint64_t size) {
  return FS(uri)->GetAsync(uri, begin, size, result_buffer);
}

galois::Result<void>
tsuba::FileStat(const std::string& uri, StatBuf* s_buf) {
  return FS(uri)->Stat(uri, s_buf);
}

galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
tsuba::FileListAsync(
    const std::string& directory, std::unordered_set<std::string>* list) {
  return FS(directory)->ListAsync(directory, list);
}

galois::Result<void>
tsuba::FileDelete(
    const std::string& directory,
    const std::unordered_set<std::string>& files) {
  return FS(directory)->Delete(directory, files);
}

galois::Result<uint8_t*>
tsuba::FileMmap(const std::string& filename, uint64_t begin, uint64_t size) {
  MappingDesc new_mapping;
  if (auto res = new_mapping.Init(filename, begin, size); !res) {
    return res.error();
  }
  std::lock_guard<std::mutex> guard(allocated_memory_mutex);
  auto [it, inserted] =
      allocated_memory.emplace(new_mapping.ptr(), std::move(new_mapping));
  if (!inserted) {
    GALOIS_LOG_ERROR("Failed to emplace! (impossible?)");
    return galois::ResultErrno();
  }
  return it->second.ptr();
}

galois::Result<void>
tsuba::FileMunmap(uint8_t* ptr) {
  std::lock_guard<std::mutex> guard(allocated_memory_mutex);
  if (allocated_memory.erase(ptr) != 1) {
    GALOIS_LOG_WARN("passed unknown pointer to tsuba_munmap");
    return tsuba::ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

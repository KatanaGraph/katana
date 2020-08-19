#ifndef GALOIS_LIBTSUBA_LOCALSTORAGE_H_
#define GALOIS_LIBTSUBA_LOCALSTORAGE_H_

#include <cstdint>
#include <string>
#include <sys/mman.h>

#include "galois/Result.h"
#include "FileStorage.h"
#include "tsuba/FileAsyncWork.h"

namespace tsuba {

/// Store byte arrays to the local file system; Provided as a convenience for
/// testing only (un-optimized)
class LocalStorage : public FileStorage {
  friend class GlobalState;
  void CleanURI(std::string* uri);
  galois::Result<void> WriteFile(std::string, const uint8_t* data,
                                 uint64_t size);
  galois::Result<void> ReadFile(std::string uri, uint64_t start, uint64_t size,
                                uint8_t* data);

public:
  LocalStorage() : FileStorage("file://") {}

  galois::Result<void> Init() override { return galois::ResultSuccess(); }
  galois::Result<void> Fini() override { return galois::ResultSuccess(); }
  galois::Result<void> Stat(const std::string& uri, StatBuf* size) override;
  galois::Result<void> Create(const std::string& uri, bool overwrite) override;

  uint32_t Priority() override { return 1; }

  galois::Result<void> GetMultiSync(const std::string& uri, uint64_t start,
                                    uint64_t size,
                                    uint8_t* result_buf) override {
    return ReadFile(uri, start, size, result_buf);
  }

  galois::Result<void> PutMultiSync(const std::string& uri, const uint8_t* data,
                                    uint64_t size) override {
    return WriteFile(uri, data, size);
  }

  // FileAsyncWork pointer can be null, otherwise contains additional work.
  // Every call to async work can potentially block (bulk synchronous parallel)
  galois::Result<std::unique_ptr<FileAsyncWork>>
  PutAsync(const std::string& uri, const uint8_t* data,
           uint64_t size) override {
    // No need for AsyncPut to local storage right now
    if (auto write_res = WriteFile(uri, data, size); !write_res) {
      return write_res.error();
    }
    return nullptr;
  }
  galois::Result<std::unique_ptr<FileAsyncWork>>
  GetAsync(const std::string& uri, uint64_t start, uint64_t size,
           uint8_t* result_buf) override {
    // I suppose there is no need for AsyncGet to local storage either
    if (auto read_res = ReadFile(uri, start, size, result_buf); !read_res) {
      return read_res.error();
    }
    return nullptr;
  }
  galois::Result<std::unique_ptr<FileAsyncWork>>
  ListAsync(const std::string& uri,
            std::unordered_set<std::string>* list) override;

  galois::Result<void>
  Delete(const std::string& directory,
         const std::unordered_set<std::string>& files) override;
};

} // namespace tsuba

#endif

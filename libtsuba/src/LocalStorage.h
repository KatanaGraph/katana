#ifndef GALOIS_LIBTSUBA_LOCAL_STORAGE_H_
#define GALOIS_LIBTSUBA_LOCAL_STORAGE_H_

#include <cstdint>
#include <string>
#include <sys/mman.h>

#include "galois/Result.h"
#include "FileStorage.h"

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
  LocalStorage() : FileStorage("file://") {}

public:
  galois::Result<void> Init() override { return galois::ResultSuccess(); }
  galois::Result<void> Fini() override { return galois::ResultSuccess(); }
  galois::Result<void> Stat(const std::string& uri, StatBuf* size) override;
  galois::Result<void> Create(const std::string& uri, bool overwrite) override;

  galois::Result<void> GetMultiSync(const std::string& uri, uint64_t start,
                                    uint64_t size,
                                    uint8_t* result_buf) override {
    return ReadFile(uri, start, size, result_buf);
  }

  galois::Result<void> PutMultiSync(const std::string& uri, const uint8_t* data,
                                    uint64_t size) override {
    return WriteFile(uri, data, size);
  }

  // Call these functions in order to do an async multipart put
  // All but the first call can block, making this a bulk synchronous parallel
  // interface
  galois::Result<void> PutMultiAsync1(const std::string& uri,
                                      const uint8_t* data,
                                      uint64_t size) override {
    return WriteFile(uri, data, size);
  }
  galois::Result<void> PutMultiAsync2([
      [maybe_unused]] const std::string& uri) override {
    return galois::ResultSuccess();
  }
  galois::Result<void> PutMultiAsync3([
      [maybe_unused]] const std::string& uri) override {
    return galois::ResultSuccess();
  }
  galois::Result<void> PutMultiAsyncFinish([
      [maybe_unused]] const std::string& uri) override {
    return galois::ResultSuccess();
  }
  galois::Result<void> PutSingleSync(const std::string& uri,
                                     const uint8_t* data,
                                     uint64_t size) override {
    return WriteFile(uri, data, size);
  }

  galois::Result<void> PutSingleAsync(const std::string& uri,
                                      const uint8_t* data,
                                      uint64_t size) override {
    return WriteFile(uri, data, size);
  }

  galois::Result<void> PutSingleAsyncFinish([
      [maybe_unused]] const std::string& uri) override {
    return galois::ResultSuccess();
  }
};

} // namespace tsuba

#endif

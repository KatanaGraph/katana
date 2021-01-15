#ifndef KATANA_LIBTSUBA_LOCALSTORAGE_H_
#define KATANA_LIBTSUBA_LOCALSTORAGE_H_

#include <sys/mman.h>

#include <cstdint>
#include <future>
#include <string>
#include <thread>

#include "katana/Result.h"
#include "tsuba/FileStorage.h"

namespace tsuba {

/// Store byte arrays to the local file system; Provided as a convenience for
/// testing only (un-optimized)
class LocalStorage : public FileStorage {
  void CleanUri(std::string* uri);
  katana::Result<void> WriteFile(
      std::string, const uint8_t* data, uint64_t size);
  katana::Result<void> ReadFile(
      std::string uri, uint64_t start, uint64_t size, uint8_t* data);
  katana::Result<void> RemoteCopyFile(
      std::string source_uri, std::string dest_uri, uint64_t begin,
      uint64_t size);

public:
  LocalStorage() : FileStorage("file://") {}

  katana::Result<void> Init() override { return katana::ResultSuccess(); }
  katana::Result<void> Fini() override { return katana::ResultSuccess(); }
  katana::Result<void> Stat(const std::string& uri, StatBuf* size) override;

  uint32_t Priority() const override { return 1; }

  katana::Result<void> GetMultiSync(
      const std::string& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override {
    return ReadFile(uri, start, size, result_buf);
  }

  katana::Result<void> PutMultiSync(
      const std::string& uri, const uint8_t* data, uint64_t size) override {
    return WriteFile(uri, data, size);
  }

  katana::Result<void> RemoteCopy(
      const std::string& source_uri, const std::string& dest_uri,
      uint64_t begin, uint64_t size) override {
    return RemoteCopyFile(source_uri, dest_uri, begin, size);
  }

  // get on future can potentially block (bulk synchronous parallel)
  std::future<katana::Result<void>> PutAsync(
      const std::string& uri, const uint8_t* data, uint64_t size) override {
    // No need for AsyncPut to local storage right now
    if (auto write_res = WriteFile(uri, data, size); !write_res) {
      return std::async(
          [=]() -> katana::Result<void> { return write_res.error(); });
    }
    return std::async(
        []() -> katana::Result<void> { return katana::ResultSuccess(); });
  }
  std::future<katana::Result<void>> GetAsync(
      const std::string& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override {
    // I suppose there is no need for AsyncGet to local storage either
    if (auto read_res = ReadFile(uri, start, size, result_buf); !read_res) {
      return std::async(
          [=]() -> katana::Result<void> { return read_res.error(); });
    }
    return std::async(
        []() -> katana::Result<void> { return katana::ResultSuccess(); });
  }
  std::future<katana::Result<void>> ListAsync(
      const std::string& uri, std::vector<std::string>* list,
      std::vector<uint64_t>* size) override;

  katana::Result<void> Delete(
      const std::string& directory,
      const std::unordered_set<std::string>& files) override;
};

}  // namespace tsuba

#endif

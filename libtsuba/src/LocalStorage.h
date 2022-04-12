#ifndef KATANA_LIBTSUBA_LOCALSTORAGE_H_
#define KATANA_LIBTSUBA_LOCALSTORAGE_H_

#include <sys/mman.h>

#include <cstdint>
#include <future>
#include <string>
#include <thread>

#include "katana/FileStorage.h"
#include "katana/Result.h"

namespace katana {

/// Store byte arrays to the local file system; Provided as a convenience for
/// testing only (un-optimized)
class LocalStorage : public FileStorage {
public:
  LocalStorage() : FileStorage("file") {}

  katana::Result<void> Init() override { return katana::ResultSuccess(); }
  katana::Result<void> Fini() override { return katana::ResultSuccess(); }
  katana::Result<void> Stat(const URI& uri, StatBuf* s_buf) override;

  uint32_t Priority() const override { return 1; }

  katana::Result<void> GetMultiSync(
      const URI& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override {
    return ReadFile(uri, start, size, result_buf);
  }

  katana::Result<void> PutMultiSync(
      const URI& uri, const uint8_t* data, uint64_t size) override {
    return WriteFile(uri, data, size);
  }

  katana::Result<void> RemoteCopy(
      const URI& source_uri, const URI& dest_uri, uint64_t begin,
      uint64_t size) override {
    return RemoteCopyFile(source_uri, dest_uri, begin, size);
  }

  // get on future can potentially block (bulk synchronous parallel)
  std::future<katana::CopyableResult<void>> PutAsync(
      const URI& uri, const uint8_t* data, uint64_t size) override {
    // No need for AsyncPut to local storage right now
    if (auto write_res = WriteFile(uri, data, size); !write_res) {
      katana::CopyableErrorInfo cei{write_res.error()};
      return std::async(
          std::launch::deferred,
          [=]() -> katana::CopyableResult<void> { return cei; });
    }
    return std::async(
        std::launch::deferred, []() -> katana::CopyableResult<void> {
          return katana::CopyableResultSuccess();
        });
  }
  std::future<katana::CopyableResult<void>> GetAsync(
      const URI& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override {
    // I suppose there is no need for AsyncGet to local storage either
    if (auto read_res = ReadFile(uri, start, size, result_buf); !read_res) {
      katana::CopyableErrorInfo cei{read_res.error()};
      return std::async(
          std::launch::deferred,
          [=]() -> katana::CopyableResult<void> { return cei; });
    }
    return std::async(
        std::launch::deferred, []() -> katana::CopyableResult<void> {
          return katana::CopyableResultSuccess();
        });
  }
  std::future<katana::CopyableResult<void>> ListAsync(
      const URI& uri, std::vector<std::string>* list,
      std::vector<uint64_t>* size) override;

  katana::Result<void> Delete(
      const URI& directory,
      const std::unordered_set<std::string>& files) override;

private:
  katana::Result<void> WriteFile(
      const URI&, const uint8_t* data, uint64_t size);
  katana::Result<void> ReadFile(
      const URI& uri, uint64_t start, uint64_t size, uint8_t* data);
  katana::Result<void> RemoteCopyFile(
      const URI& source_uri, const URI& dest_uri, uint64_t begin,
      uint64_t size);
};

}  // namespace katana

#endif

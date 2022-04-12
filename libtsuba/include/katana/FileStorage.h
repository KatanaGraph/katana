#ifndef KATANA_LIBTSUBA_KATANA_FILESTORAGE_H_
#define KATANA_LIBTSUBA_KATANA_FILESTORAGE_H_

#include <cstdint>
#include <future>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/config.h"

namespace katana {

struct StatBuf;

class KATANA_EXPORT FileStorage {
  std::string uri_scheme_;

protected:
  FileStorage(std::string_view uri_scheme) : uri_scheme_(uri_scheme) {}

public:
  FileStorage(const FileStorage& no_copy) = delete;
  FileStorage(FileStorage&& no_move) = delete;
  FileStorage& operator=(const FileStorage& no_copy) = delete;
  FileStorage& operator=(FileStorage&& no_move) = delete;
  virtual ~FileStorage();

  std::string_view uri_scheme() const { return uri_scheme_; }
  virtual katana::Result<void> Init() = 0;
  virtual katana::Result<void> Fini() = 0;
  virtual katana::Result<void> Stat(const URI& uri, StatBuf* size) = 0;

  virtual katana::Result<void> GetMultiSync(
      const URI& uri, uint64_t start, uint64_t size, uint8_t* result_buf) = 0;

  virtual katana::Result<void> PutMultiSync(
      const URI& uri, const uint8_t* data, uint64_t size) = 0;

  virtual katana::Result<void> RemoteCopy(
      const URI& source_uri, const URI& dest_uri, uint64_t begin,
      uint64_t size) = 0;

  /// Storage classes with higher priority will be tried by GlobalState earlier
  /// currently only used to enforce local fs default; GlobalState defaults
  /// to the LocalStorage when no protocol on the URI is provided
  virtual uint32_t Priority() const { return 0; }

  // get on future can potentially block (bulk synchronous parallel)
  virtual std::future<katana::CopyableResult<void>> PutAsync(
      const URI& uri, const uint8_t* data, uint64_t size) = 0;
  virtual std::future<katana::CopyableResult<void>> GetAsync(
      const URI& uri, uint64_t start, uint64_t size, uint8_t* result_buf) = 0;
  virtual std::future<katana::CopyableResult<void>> ListAsync(
      const URI& directory, std::vector<std::string>* list,
      std::vector<uint64_t>* size) = 0;
  virtual katana::Result<void> Delete(
      const URI& directory, const std::unordered_set<std::string>& files) = 0;
};

/// RegisterFileStorage adds a file storage backend to the tsuba library. File
/// storage backends must be registered before katana::InitTsuba. Backends need to be
/// registered for each katana::InitTsuba call.
KATANA_EXPORT void RegisterFileStorage(FileStorage* fs);

}  // namespace katana

#endif

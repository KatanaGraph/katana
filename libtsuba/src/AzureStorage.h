#ifndef GALOIS_LIBTSUBA_AZURESTORAGE_H_
#define GALOIS_LIBTSUBA_AZURESTORAGE_H_

#include <future>

#include "FileStorage.h"

namespace tsuba {

class AzureStorage : public FileStorage {
  galois::Result<std::pair<std::string, std::string>> CleanUri(
      const std::string& uri);

public:
  // "abfs://" is the uri style used by the hadoop plug in for azure blob store
  // https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri
  AzureStorage() : FileStorage("abfs://") {}

  galois::Result<void> Init() override;
  galois::Result<void> Fini() override;
  galois::Result<void> Stat(const std::string& uri, StatBuf* s_buf) override;
  galois::Result<void> Create(const std::string& uri, bool overwrite) override;

  galois::Result<void> GetMultiSync(
      const std::string& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override;

  galois::Result<void> PutMultiSync(
      const std::string& uri, const uint8_t* data, uint64_t size) override;

  // get on future can potentially block (bulk synchronous parallel)
  galois::Result<std::future<galois::Result<void>>> PutAsync(
      const std::string& uri, const uint8_t* data, uint64_t size) override;
  galois::Result<std::future<galois::Result<void>>> GetAsync(
      const std::string& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override;
  galois::Result<std::future<galois::Result<void>>> ListAsync(
      const std::string& directory, std::vector<std::string>* list,
      std::vector<uint64_t>* size) override;
  galois::Result<void> Delete(
      const std::string& directory,
      const std::unordered_set<std::string>& files) override;
};

}  // namespace tsuba

#endif

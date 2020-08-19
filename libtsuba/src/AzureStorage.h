#ifndef GALOIS_LIBTSUBA_AZURESTORAGE_H_
#define GALOIS_LIBTSUBA_AZURESTORAGE_H_

#include "FileStorage.h"

namespace tsuba {

class AzureStorage : public FileStorage {
  galois::Result<std::pair<std::string, std::string>>
  CleanURI(const std::string& uri);

public:
  // "abfs://" is the uri style used by the hadoop plug in for azure blob store
  // https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri
  AzureStorage() : FileStorage("abfs://") {}

  galois::Result<void> Init() override;
  galois::Result<void> Fini() override;
  galois::Result<void> Stat(const std::string& uri, StatBuf* s_buf) override;
  galois::Result<void> Create(const std::string& uri, bool overwrite) override;

  galois::Result<void> GetMultiSync(const std::string& uri, uint64_t start,
                                    uint64_t size,
                                    uint8_t* result_buf) override;

  galois::Result<void> PutMultiSync(const std::string& uri, const uint8_t* data,
                                    uint64_t size) override;

  // FileAsyncWork pointer can be null, otherwise contains additional work.
  // Every call to async work can potentially block (bulk synchronous parallel)
  galois::Result<std::unique_ptr<FileAsyncWork>>
  PutAsync(const std::string& uri, const uint8_t* data, uint64_t size) override;
  galois::Result<std::unique_ptr<FileAsyncWork>>
  GetAsync(const std::string& uri, uint64_t start, uint64_t size,
           uint8_t* result_buf) override;
  galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
  ListAsync(const std::string& directory,
            std::unordered_set<std::string>* list) override;
  galois::Result<void>
  Delete(const std::string& directory,
         const std::unordered_set<std::string>& files) override;
};

} // namespace tsuba

#endif

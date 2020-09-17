#ifndef GALOIS_LIBTSUBA_S3STORAGE_H_
#define GALOIS_LIBTSUBA_S3STORAGE_H_

#include <cstdint>

#include "FileStorage.h"
#include "galois/Result.h"

namespace tsuba {

class S3Storage : public FileStorage {
  friend class GlobalState;
  galois::Result<std::pair<std::string, std::string>> CleanURI(
      const std::string& uri);

public:
  S3Storage() : FileStorage("s3://") {}

  galois::Result<void> Init() override;
  galois::Result<void> Fini() override;
  galois::Result<void> Stat(const std::string& uri, StatBuf* s_buf) override;
  galois::Result<void> Create(const std::string& uri, bool overwrite) override;

  galois::Result<void> GetMultiSync(
      const std::string& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override;

  galois::Result<void> PutMultiSync(
      const std::string& uri, const uint8_t* data, uint64_t size) override;

  // FileAsyncWork pointer can be null, otherwise contains additional work.
  // Every call to async work can potentially block (bulk synchronous parallel)
  galois::Result<std::unique_ptr<FileAsyncWork>> PutAsync(
      const std::string& uri, const uint8_t* data, uint64_t size) override;
  galois::Result<std::unique_ptr<FileAsyncWork>> GetAsync(
      const std::string& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override;
  galois::Result<std::unique_ptr<tsuba::FileAsyncWork>> ListAsync(
      const std::string& uri, std::unordered_set<std::string>* list) override;
  // files are relative to uri pseudo-directory or bucket
  galois::Result<void> Delete(
      const std::string& uri,
      const std::unordered_set<std::string>& files) override;
};

}  // namespace tsuba

#endif

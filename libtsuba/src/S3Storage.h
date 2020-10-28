#ifndef GALOIS_LIBTSUBA_S3STORAGE_H_
#define GALOIS_LIBTSUBA_S3STORAGE_H_

#include <cstdint>
#include <future>

#include "FileStorage.h"
#include "galois/Result.h"
#include "tsuba/s3_internal.h"

namespace tsuba {

class S3Storage : public FileStorage {
  friend class GlobalState;
  galois::Result<std::pair<std::string, std::string>> CleanUri(
      const std::string& uri);

  internal::S3Client s3_client;

public:
  S3Storage() : FileStorage("s3://") {}

  galois::Result<void> Init() override;
  galois::Result<void> Fini() override;
  galois::Result<void> Stat(const std::string& uri, StatBuf* s_buf) override;

  galois::Result<void> GetMultiSync(
      const std::string& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override;

  galois::Result<void> PutMultiSync(
      const std::string& uri, const uint8_t* data, uint64_t size) override;

  // get on future can potentially block (bulk synchronous parallel)
  std::future<galois::Result<void>> PutAsync(
      const std::string& uri, const uint8_t* data, uint64_t size) override;
  std::future<galois::Result<void>> GetAsync(
      const std::string& uri, uint64_t start, uint64_t size,
      uint8_t* result_buf) override;
  std::future<galois::Result<void>> ListAsync(
      const std::string& uri, std::vector<std::string>* list,
      std::vector<uint64_t>* size) override;
  // files are relative to uri pseudo-directory or bucket
  galois::Result<void> Delete(
      const std::string& uri,
      const std::unordered_set<std::string>& files) override;
};

}  // namespace tsuba

#endif

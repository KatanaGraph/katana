#ifndef GALOIS_LIBTSUBA_GSSTORAGE_H_
#define GALOIS_LIBTSUBA_GSSTORAGE_H_

#include <future>

#include "FileStorage.h"

namespace tsuba {

class GSStorage : public FileStorage {
  galois::Result<std::pair<std::string, std::string>> CleanUri(
      const std::string& uri);

public:
  // Google uses "gs://" for their gcs URIs
  GSStorage() : FileStorage("gs://") {}

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
      const std::string& directory, std::vector<std::string>* list,
      std::vector<uint64_t>* size) override;
  galois::Result<void> Delete(
      const std::string& directory,
      const std::unordered_set<std::string>& files) override;
};

}  // namespace tsuba

#endif

#ifndef GALOIS_LIBTSUBA_FILESTORAGE_H_
#define GALOIS_LIBTSUBA_FILESTORAGE_H_

#include <cstdint>
#include <string_view>

#include "galois/Result.h"
#include "tsuba/FileAsyncWork.h"

namespace tsuba {

struct StatBuf;

class FileStorage {
  std::string uri_scheme_;

protected:
  FileStorage(std::string_view uri_scheme) : uri_scheme_(uri_scheme) {}

public:
  FileStorage(const FileStorage& no_copy) = delete;
  FileStorage(FileStorage&& no_move)      = delete;
  FileStorage& operator=(const FileStorage& no_copy) = delete;
  FileStorage& operator=(FileStorage&& no_move) = delete;
  virtual ~FileStorage()                        = default;

  std::string_view uri_scheme() { return uri_scheme_; }
  virtual galois::Result<void> Init()                                      = 0;
  virtual galois::Result<void> Fini()                                      = 0;
  virtual galois::Result<void> Stat(const std::string& uri, StatBuf* size) = 0;
  virtual galois::Result<void> Create(const std::string& uri,
                                      bool overwrite)                      = 0;

  virtual galois::Result<void> GetMultiSync(const std::string& uri,
                                            uint64_t start, uint64_t size,
                                            uint8_t* result_buf) = 0;

  virtual galois::Result<void>
  PutMultiSync(const std::string& uri, const uint8_t* data, uint64_t size) = 0;

  // FileAsyncWork pointer can be null, otherwise contains additional work.
  // Every call to async work can potentially block (bulk synchronous parallel)
  virtual galois::Result<std::unique_ptr<FileAsyncWork>>
  PutAsync(const std::string& uri, const uint8_t* data, uint64_t size) = 0;
};

} // namespace tsuba

#endif

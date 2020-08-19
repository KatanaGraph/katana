#ifndef GALOIS_LIBTSUBA_TSUBA_FILEASYNCWORK_H_
#define GALOIS_LIBTSUBA_TSUBA_FILEASYNCWORK_H_

#include "galois/Result.h"
#include "galois/Logging.h"

namespace tsuba {

class FileAsyncWork {
  // This holds the result of directory listings.
  std::vector<std::string> list_out_{};

protected:
public:
  FileAsyncWork() {}
  FileAsyncWork(const FileAsyncWork& no_copy) = delete;
  FileAsyncWork(FileAsyncWork&& no_move)      = delete;
  FileAsyncWork& operator=(const FileAsyncWork& no_copy) = delete;
  FileAsyncWork& operator=(FileAsyncWork&& no_move) = delete;
  virtual ~FileAsyncWork()                          = default;

  // Call next async function in the chain
  virtual galois::Result<void> operator()() {
    GALOIS_LOG_VASSERT(false, "No default operator() in FileAsyncWork");
  }
  virtual bool Done() const { return true; }

  // Location of directory listing
  virtual std::vector<std::string>& GetListOutRef() { return list_out_; }
};

} // namespace tsuba

#endif

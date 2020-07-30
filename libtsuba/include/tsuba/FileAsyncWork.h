#ifndef GALOIS_LIBTSUBA_TSUBA_FILEASYNCWORK_H_
#define GALOIS_LIBTSUBA_TSUBA_FILEASYNCWORK_H_

#include "galois/Result.h"

namespace tsuba {

class FileAsyncWork {
protected:
  FileAsyncWork() {}

public:
  FileAsyncWork(const FileAsyncWork& no_copy) = delete;
  FileAsyncWork(FileAsyncWork&& no_move)      = delete;
  FileAsyncWork& operator=(const FileAsyncWork& no_copy) = delete;
  FileAsyncWork& operator=(FileAsyncWork&& no_move) = delete;
  virtual ~FileAsyncWork()                          = default;

  // Call next async function in the chain
  virtual galois::Result<void> operator()() = 0;
  virtual bool Done() const                 = 0;
};

} // namespace tsuba

#endif

#ifndef GALOIS_LIBTSUBA_TSUBA_FILEASYNCWORK_H_
#define GALOIS_LIBTSUBA_TSUBA_FILEASYNCWORK_H_

#include <future>

#include "galois/Result.h"
#include "galois/Logging.h"

namespace tsuba {

class FileAsyncWork {
  std::future<galois::Result<void>> future_;
  bool done_;

public:
  FileAsyncWork(std::future<galois::Result<void>> future)
      : future_(std::move(future)), done_(false) {}
  FileAsyncWork(const FileAsyncWork& no_copy) = delete;
  FileAsyncWork(FileAsyncWork&& no_move)      = delete;
  FileAsyncWork& operator=(const FileAsyncWork& no_copy) = delete;
  FileAsyncWork& operator=(FileAsyncWork&& no_move) = delete;
  virtual ~FileAsyncWork()                          = default;

  virtual galois::Result<void> operator()() {
    auto outcome = future_.get();
    done_        = true;
    return outcome;
  }
  virtual bool Done() const { return done_; }
  galois::Result<void> Finish() {
    while (!Done()) {
      auto res = this->operator()();
      if (!res) {
        return res;
      }
    }
    return galois::ResultSuccess();
  }
};

} // namespace tsuba

#endif

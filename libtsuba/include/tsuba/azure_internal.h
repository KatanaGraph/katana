#ifndef GALOIS_LIBTSUBA_TSUBA_AZUREINTERNAL_H_
#define GALOIS_LIBTSUBA_TSUBA_AZUREINTERNAL_H_

#include <future>
#include <string>

#include <storage_outcome.h>

#include "blob/blob_client.h"
#include "galois/Result.h"
#include "tsuba/FileAsyncWork.h"
#include "tsuba/Errors.h"

// Don't call these directly.  They are intended for use only in azure.cpp and
// testing code

namespace tsuba::internal {

class AzureAsyncWork : public FileAsyncWork {
  std::future<galois::Result<void>> future_;
  bool done_;

public:
  AzureAsyncWork(std::future<galois::Result<void>> future)
      : future_(std::move(future)), done_(false) {}

  galois::Result<void> operator()() override {
    auto outcome = future_.get();
    done_        = true;
    return outcome;
  }
  bool Done() const override { return done_; }
};

} // namespace tsuba::internal

#endif

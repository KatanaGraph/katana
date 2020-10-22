#ifndef GALOIS_LIBTSUBA_TSUBA_S3INTERNAL_H_
#define GALOIS_LIBTSUBA_TSUBA_S3INTERNAL_H_

#include <condition_variable>
#include <mutex>
#include <stack>
#include <string>

#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"

// Don't call these directly.  They are intended for use only in s3.cpp and
// testing code

namespace tsuba::internal {

class CountingSemaphore {
  std::mutex mutex_{};
  std::condition_variable cv_{};
  uint64_t goal_{
      UINT64_C(0)};  // Goal initialized > 0, when reaches 0 we are done

public:
  CountingSemaphore(const CountingSemaphore& no_copy) = delete;
  CountingSemaphore(CountingSemaphore&& no_move) = delete;
  CountingSemaphore& operator=(const CountingSemaphore& no_copy) = delete;
  CountingSemaphore& operator=(CountingSemaphore&& no_move) = delete;

  // We find out goal after initialization
  CountingSemaphore() {}

  void SetGoal(uint64_t goal) {
    GALOIS_LOG_VASSERT(
        goal > UINT64_C(0), "Count of CountingSemaphore must be > 0");
    goal_ = goal;
  }
  void GoalMinusOne() {
    std::unique_lock<std::mutex> lk(mutex_);
    GALOIS_LOG_VASSERT(
        goal_ > UINT64_C(0),
        "Goal CountingSemaphore is 0, but in GoalMinusOne");
    goal_--;
    lk.unlock();  // Notify without lock
    cv_.notify_one();
  }
  void WaitGoal() {
    std::unique_lock<std::mutex> lk(mutex_);
    cv_.wait(lk, [&] { return goal_ == (uint64_t)0; });
  }
};

struct PutMultiImpl;
struct PutMultiHandle {
  PutMultiImpl* impl_;
};

GALOIS_EXPORT galois::Result<void> S3GetMultiAsync(
    const std::string& bucket, const std::string& object, uint64_t start,
    uint64_t size, uint8_t* result_buf, CountingSemaphore* sema);
GALOIS_EXPORT void S3GetMultiAsyncFinish(CountingSemaphore* sema);

GALOIS_EXPORT galois::Result<void> S3PutSingleSync(
    const std::string& bucket, const std::string& object, const uint8_t* data,
    uint64_t size);
GALOIS_EXPORT PutMultiHandle S3PutMultiAsync1(
    const std::string& bucket, const std::string& object, const uint8_t* data,
    uint64_t size);

GALOIS_EXPORT galois::Result<void> S3PutMultiAsync2(
    const std::string& bucket, const std::string& object, PutMultiHandle pmh);
GALOIS_EXPORT galois::Result<void> S3PutMultiAsync3(
    const std::string& bucket, const std::string& object, PutMultiHandle pmh);
GALOIS_EXPORT galois::Result<void> S3PutMultiAsyncFinish(
    const std::string& bucket, const std::string& object, PutMultiHandle pmh);

GALOIS_EXPORT galois::Result<void> S3PutSingleAsync(
    const std::string& bucket, const std::string& object, const uint8_t* data,
    uint64_t size, CountingSemaphore* sema);
GALOIS_EXPORT void S3PutSingleAsyncFinish(CountingSemaphore* sema);

// Google storage compatability
GALOIS_EXPORT std::future<galois::Result<void>> S3ListAsyncV1(
    const std::string& bucket, const std::string& object,
    std::vector<std::string>* list, std::vector<uint64_t>* size);
GALOIS_EXPORT galois::Result<void> S3SingleDelete(
    const std::string& bucket, const std::string& object);

}  // namespace tsuba::internal

#endif

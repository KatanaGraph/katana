#ifndef GALOIS_LIBTSUBA_S3_INTERNAL_H_
#define GALOIS_LIBTSUBA_S3_INTERNAL_H_

#include "galois/Result.h"
#include "galois/ErrorCode.h"
#include <string>
#include <mutex>
#include <condition_variable>
#include <stack>

// Don't call these directly.  They are intended for use only in s3.cpp and
// testing code

namespace tsuba::internal {

// Remember what bucket and object we are operating on and store a stack of
// functions to call until we are done with our work.  Any call (except the
// first) might block and there is no interface to determine if you will block.
class S3AsyncWork : public FileAsyncWork {
  std::string bucket_{};
  std::string object_{};
  std::mutex mutex_{};
  std::condition_variable cv_{};
  uint64_t goal_{0UL}; // Goal initialized > 0, when reaches 0 we are done

  std::stack<galois::Result<void> (*)(S3AsyncWork& s3aw)> func_stack_{};

public:
  S3AsyncWork(const std::string& bucket, const std::string& object)
      : bucket_(bucket), object_(object) {}
  ~S3AsyncWork() {}

  std::string GetBucket() const { return bucket_; }
  std::string GetObject() const { return object_; }

  void Push(galois::Result<void> (*func)(S3AsyncWork& s3aw)) {
    func_stack_.push(func);
  }
  void SetGoal(uint64_t goal) {
    GALOIS_LOG_VASSERT(goal > 0UL, "Count of FileAsyncWork must be > 0");
    goal_ = goal;
  }
  void GoalMinusOne() {
    std::unique_lock<std::mutex> lk(mutex_);
    GALOIS_LOG_VASSERT(goal_ > 0UL,
                       "Goal FileAsyncWork is 0, but in GoalMinusOne");
    goal_--;
    lk.unlock(); // Notify without lock
    cv_.notify_one();
  }
  void WaitGoal() {
    std::unique_lock<std::mutex> lk(mutex_);
    cv_.wait(lk, [&] { return goal_ == (uint64_t)0; });
  }

  // Call next async function in the chain
  galois::Result<void> operator()() {
    if (!func_stack_.empty()) {
      auto func = func_stack_.top();
      auto res  = func(*this);
      if (!res) {
        // TODO: We should abort multi-part uploads
      }
      func_stack_.pop();
      return res;
    }
    return support::ErrorCode::InvalidArgument;
  }
  bool Done() const { return func_stack_.empty(); }
};

galois::Result<void> S3GetSingleAsync(S3AsyncWork& s3aw, uint64_t start,
                                      uint64_t size, uint8_t* result_buf);
galois::Result<void> S3GetSingleAsyncFinish(S3AsyncWork& s3aw);

galois::Result<void> S3PutSingleSync(const std::string& bucket,
                                     const std::string& object,
                                     const uint8_t* data, uint64_t size);
galois::Result<void> S3PutMultiAsync1(S3AsyncWork& s3aw, const uint8_t* data,
                                      uint64_t size);
galois::Result<void> S3PutMultiAsync2(S3AsyncWork& s3aw);
galois::Result<void> S3PutMultiAsync3(S3AsyncWork& s3aw);
galois::Result<void> S3PutMultiAsyncFinish(S3AsyncWork& s3aw);

galois::Result<void> S3PutSingleAsync(S3AsyncWork& s3aw, const uint8_t* data,
                                      uint64_t size);
galois::Result<void> S3PutSingleAsyncFinish(S3AsyncWork& s3aw);
} // namespace tsuba::internal

#endif // GALOIS_LIBTSUBA_S3_INTERNAL_H_

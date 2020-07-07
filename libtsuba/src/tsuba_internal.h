#ifndef GALOIS_LIBTSUBA_TSUBA_INTERNAL_H_
#define GALOIS_LIBTSUBA_TSUBA_INTERNAL_H_

#include <cerrno>
#include <thread>
#include <cstdint>
#include <cassert>

#include "galois/CommBackend.h"

constexpr const uint64_t kKBShift = 10;
constexpr const uint64_t kMBShift = 20;
constexpr const uint64_t kGBShift = 30;

template <typename T>
constexpr T KB(T v) {
  return v << kKBShift;
}
template <typename T>
constexpr T MB(T v) {
  return v << kMBShift;
}
template <typename T>
constexpr T GB(T v) {
  return v << kGBShift;
}

namespace tsuba {

class GlobalState {
  static std::unique_ptr<GlobalState> ref;

  galois::CommBackend* comm;

  GlobalState(galois::CommBackend* new_comm) : comm(new_comm){};

public:
  GlobalState(const GlobalState& no_copy)  = delete;
  GlobalState(const GlobalState&& no_move) = delete;
  GlobalState& operator=(const GlobalState& no_copy) = delete;
  GlobalState& operator=(const GlobalState&& no_move) = delete;

  ~GlobalState() = default;

  galois::CommBackend* Comm() const {
    assert(comm != nullptr);
    return comm;
  }

  static void Init(galois::CommBackend* comm) {
    assert(ref == nullptr);
    // new instead of make_unique to keep constructor private
    auto new_ref = new GlobalState(comm);
    ref.reset(new_ref);
  }
  static void Fini() { ref.reset(nullptr); }
  static const GlobalState& Get() {
    assert(ref != nullptr);
    return *ref;
  }
};

/* set errno and return */
template <typename T>
static inline T ERRNO_RET(int errno_val, T ret) {
  errno = errno_val;
  return ret;
}

} /* namespace tsuba */

#endif

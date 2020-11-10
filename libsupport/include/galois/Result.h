#ifndef GALOIS_LIBSUPPORT_GALOIS_RESULT_H_
#define GALOIS_LIBSUPPORT_GALOIS_RESULT_H_

#include <cassert>
#include <cerrno>
#include <future>

#include <boost/outcome/outcome.hpp>

namespace galois {

template <class T>
using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<T>;

static inline auto
ResultSuccess() {
  return BOOST_OUTCOME_V2_NAMESPACE::success();
}

static inline auto
ResultErrno() {
  assert(errno);
  return std::error_code(errno, std::system_category());
}

template <typename ResType, typename ErrType>
static inline std::future<Result<ResType>>
AsyncError(ErrType errCode) {
  // deferred to try and avoid dispatch since there's no async work to do
  return std::async(std::launch::deferred, [=]() -> galois::Result<ResType> {
    return errCode;
  });
}

}  // namespace galois

#endif

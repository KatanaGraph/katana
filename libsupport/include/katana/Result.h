#ifndef KATANA_LIBSUPPORT_KATANA_RESULT_H_
#define KATANA_LIBSUPPORT_KATANA_RESULT_H_

#include <cassert>
#include <cerrno>
#include <future>

#include <boost/outcome/outcome.hpp>

#include "katana/Logging.h"

namespace katana {

template <class T>
using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<T>;

static inline auto
ResultSuccess() {
  return BOOST_OUTCOME_V2_NAMESPACE::success();
}

static inline auto
ResultErrno() {
  KATANA_LOG_DEBUG_ASSERT(errno);
  return std::error_code(errno, std::system_category());
}

template <typename ResType, typename ErrType>
static inline std::future<Result<ResType>>
AsyncError(ErrType errCode) {
  // deferred to try and avoid dispatch since there's no async work to do
  return std::async(std::launch::deferred, [=]() -> katana::Result<ResType> {
    return errCode;
  });
}

}  // namespace katana

#endif

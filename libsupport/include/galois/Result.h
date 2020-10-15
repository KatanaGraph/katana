#ifndef GALOIS_LIBSUPPORT_GALOIS_RESULT_H_
#define GALOIS_LIBSUPPORT_GALOIS_RESULT_H_

#include <cerrno>

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
  return std::error_code(errno, std::system_category());
}

}  // namespace galois

#endif

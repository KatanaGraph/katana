#ifndef GALOIS_LIBSUPPORT_GALOIS_RESULT_H_
#define GALOIS_LIBSUPPORT_GALOIS_RESULT_H_

#include <boost/outcome/outcome.hpp>

namespace galois {

template <class T>
using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<T>;

static inline auto ResultSuccess() {
  return BOOST_OUTCOME_V2_NAMESPACE::success();
}

} // namespace galois

// NOTE: on older compilers auto conversion to Result will fail for types
// that can't be copied. We've adopted the workaround of returning such objects
// like so:
//
// Result<Thing> MakeMoveOnlyThing() {
//   Thing t;
//   ...
//   ...
//   ...
//   return Thing(std::move(t));
// }
//
// Other wise builds on the supported compiler: GCC 7 will fail.
//
// TODO(all) fix instances of this when we drop GCC 7 since it is not required
// after that point.

#endif

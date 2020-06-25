#ifndef GALOIS_LIBSUPPORT_GALOIS_RESULT_H_
#define GALOIS_LIBSUPPORT_GALOIS_RESULT_H_

#include <boost/outcome/outcome.hpp>

namespace galois {

template <class T>
using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<T>;

}

#endif

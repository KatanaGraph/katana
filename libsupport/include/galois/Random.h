#ifndef GALOIS_LIBSUPPORT_GALOIS_RANDOM_H_
#define GALOIS_LIBSUPPORT_GALOIS_RANDOM_H_

#include <string>

#include "galois/config.h"

namespace galois {

GALOIS_EXPORT std::string RandomAlphanumericString(uint64_t len);
// Range 0..len-1, inclusive
GALOIS_EXPORT int64_t RandomUniformInt(int64_t len);
// Range min+1..max-1, inclusive
GALOIS_EXPORT int64_t RandomUniformInt(int64_t min, int64_t max);
// Range 0.0f..max, inclusive
GALOIS_EXPORT float RandomUniformFloat(float max);

} // namespace galois

#endif

#ifndef KATANA_LIBSUPPORT_KATANA_RANDOM_H_
#define KATANA_LIBSUPPORT_KATANA_RANDOM_H_

#include <string>

#include "katana/config.h"

namespace katana {

KATANA_EXPORT std::string RandomAlphanumericString(uint64_t len);
// Range 0..len-1, inclusive
KATANA_EXPORT int64_t RandomUniformInt(int64_t len);
// Range min+1..max-1, inclusive
KATANA_EXPORT int64_t RandomUniformInt(int64_t min, int64_t max);
// Range 0.0f..max, inclusive
KATANA_EXPORT float RandomUniformFloat(float max);

}  // namespace katana

#endif

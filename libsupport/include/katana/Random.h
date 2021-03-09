#ifndef KATANA_LIBSUPPORT_KATANA_RANDOM_H_
#define KATANA_LIBSUPPORT_KATANA_RANDOM_H_

#include <random>
#include <string>

#include "katana/config.h"

namespace katana {

using RandGenerator = std::mt19937;

/// Generate a random alphanumeric string of length \param len using
/// \param gen if provided. If no generator is specified, use the output of
/// GetGenerator
KATANA_EXPORT std::string RandomAlphanumericString(
    uint64_t len, RandGenerator* gen = nullptr);

/// \returns a random number generator seeded with randomness from the platform.
/// The generator is local to the calling thread so uses of it are thread safe.
/// Useful for things like `std::uniform_int_distribution`
KATANA_EXPORT RandGenerator& GetGenerator();

}  // namespace katana

#endif

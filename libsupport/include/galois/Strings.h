#ifndef GALOIS_LIBSUPPORT_GALOIS_STRINGS_H_
#define GALOIS_LIBSUPPORT_GALOIS_STRINGS_H_

#include <string>

#include "galois/config.h"

/// @file Strings.h
///
/// Basic string manipulation functions for situations where you can tolerate
/// some string copies in exchange for a clear API.
///
/// C++20 will have string.starts_with and string.ends_with.

namespace galois {

/// TrimPrefix returns a string without the given prefix. If the string does
/// not have the prefix, return the string unchanged.
GALOIS_EXPORT std::string TrimPrefix(
    const std::string& s, const std::string& prefix);

GALOIS_EXPORT bool HasPrefix(const std::string& s, const std::string& prefix);

/// TrimSuffix returns a string without the given suffix. If the string does
/// not have the suffix, return the string unchanged.
GALOIS_EXPORT std::string TrimSuffix(
    const std::string& s, const std::string& suffix);

GALOIS_EXPORT bool HasSuffix(const std::string& s, const std::string& suffix);

}  // namespace galois

#endif

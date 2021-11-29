#ifndef KATANA_LIBSUPPORT_KATANA_STRINGS_H_
#define KATANA_LIBSUPPORT_KATANA_STRINGS_H_

#include <initializer_list>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "katana/config.h"

/// @file Strings.h
///
/// Basic string manipulation functions for situations where you can tolerate
/// some string copies in exchange for a clear API.
///
/// C++20 will have string.starts_with and string.ends_with.

namespace katana {

/// FromBase64 converts from base64 string into a binary encoded string
/// \param input base64 encoded input string
KATANA_EXPORT std::string FromBase64(const std::string& input);

/// ToBase64 encodes message string into a Base64 string.
/// \param url_safe forces URL-safe encoding of result base64 result (replacing +/ with -_)
/// \param message binary string input
KATANA_EXPORT std::string ToBase64(
    const std::string& message, bool url_safe = false);

/// TrimPrefix returns a string without the given prefix. If the string does
/// not have the prefix, return the string unchanged.
KATANA_EXPORT std::string TrimPrefix(
    const std::string& s, const std::string& prefix);

KATANA_EXPORT bool HasPrefix(const std::string& s, const std::string& prefix);

/// TrimSuffix returns a string without the given suffix. If the string does
/// not have the suffix, return the string unchanged.
KATANA_EXPORT std::string TrimSuffix(
    const std::string& s, const std::string& suffix);

KATANA_EXPORT bool HasSuffix(const std::string& s, const std::string& suffix);

/// SplitView returns a list of words in \param s using \param sep as the
/// delimiter string. Splits at most \param max times (so there will be at most
/// max + 1 entries in the output)
KATANA_EXPORT std::vector<std::string_view> SplitView(
    std::string_view s, std::string_view sep,
    uint64_t max = std::numeric_limits<uint64_t>::max());

/// Join returns a string that is the concatenation of every item from begin to
/// end with items separated by sep.
///
/// Join mirrors the functionality of fmt::join at the expense of an additional
/// copy but works around a bug in fmt::join (fmt<8) where items that only
/// overload operator<<(ostream, T) cannot be joined.
template <typename It>
std::string
Join(It begin, It end, std::string_view sep) {
  if (begin == end) {
    return "";
  }

  std::stringstream out;

  for (; begin != end;) {
    out << *begin;

    ++begin;

    if (begin != end) {
      out << sep;
    }
  }

  return out.str();
}

/// Join returns a string that is the concatenation of every item in items
/// with items separated by sep.
template <typename Range>
std::string
Join(const Range& items, std::string_view sep) {
  return Join(items.begin(), items.end(), sep);
}

/// Join returns a string that is the concatenation of every item in items
/// with items separated by sep.
template <typename T>
std::string
Join(const std::initializer_list<T>& items, std::string_view sep) {
  return Join(items.begin(), items.end(), sep);
}

}  // namespace katana

#endif

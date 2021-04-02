#ifndef KATANA_LIBSUPPORT_KATANA_STRINGS_H_
#define KATANA_LIBSUPPORT_KATANA_STRINGS_H_

#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>
#include <sstream>

#include <fmt/format.h>

#include "katana/config.h"
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/archive/iterators/insert_linebreaks.hpp>
#include <boost/archive/iterators/remove_whitespace.hpp>



/// @file Strings.h
///
/// Basic string manipulation functions for situations where you can tolerate
/// some string copies in exchange for a clear API.
///
/// C++20 will have string.starts_with and string.ends_with.

namespace katana {

/// FromBase64 decodes \param input into a Base64 encoded string
KATANA_EXPORT std::string FromBase64(std::string input);

/// ToBase64 encodes \param message string into a Base64 string
KATANA_EXPORT std::string ToBase64(std::string message);

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

/// Join returns a string that is the concatenation of every object from
/// \param begin to \param end  all separated by an instance of \param sep
template <typename I>
std::string
Join(std::string_view sep, I begin, I end) {
  if (begin == end) {
    return "";
  }
  fmt::memory_buffer buf;
  auto last = --end;
  for (auto it = begin; it != last; ++it) {
    fmt::format_to(buf, "{}{}", *it, sep);
  }
  fmt::format_to(buf, "{}", *last);
  return to_string(buf);
}

/// Join returns a string that is the concatenation of every object in
/// \param items all separated by an instance of \param sep
template <typename C>
std::string
Join(std::string_view sep, const C& items) {
  return Join(sep, items.begin(), items.end());
}

/// Join returns a string that is the concatenation of every object in
/// \param items all separated by an instance of \param sep
template <typename T>
std::string
Join(std::string_view sep, const std::initializer_list<T> items) {
  return Join(sep, items.begin(), items.end());
}

}  // namespace katana

#endif

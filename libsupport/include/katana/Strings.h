#ifndef KATANA_LIBSUPPORT_KATANA_STRINGS_H_
#define KATANA_LIBSUPPORT_KATANA_STRINGS_H_

#include <initializer_list>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>

#include "katana/config.h"

/// @file Strings.h
///
/// Basic string manipulation functions for situations where you can tolerate
/// some string copies in exchange for a clear API.
///
/// C++20 will have string.starts_with and string.ends_with.

namespace katana {

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

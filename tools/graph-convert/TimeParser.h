#ifndef GALOIS_TOOLS_GRAPH_CONVERT_TIME_PARSER_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_TIME_PARSER_H_

#include <array>
#include <optional>
#include <chrono>
#include <sstream>

#include <date/date.h>

namespace galois {

/// A TimeParser parses various string formats into a Unix timestamp.
///
/// \tparam Duration a std::chrono::duration indicating the resolution/units of
/// the returned timestamps
template <typename Duration = std::chrono::milliseconds>
class TimeParser {
  int last_format_{0};

public:
  /// Parse parses a string and returns a Unix timestamp in units of Duration.
  /// If the string could not be parsed, the null option is returned.
  std::optional<int64_t> Parse(const std::string& str);
};

} // namespace galois

template <typename Duration>
std::optional<int64_t>
galois::TimeParser<Duration>::Parse(const std::string& str) {
  // Possible time formats
  //
  // ISO 8601:
  //  2020-11-22T11:22:33.52Z or
  //  2020-11-22 11:22:33.52Z
  //
  // RFC 3339:
  //  2020-11-22 11:22:33.52Z only
  std::array formats{
      "%F %H:%M:%SZ", // RFC 3339 UTC
      "%FT%H:%M:%SZ", // ISO 8601 UTC
      "%FT%H:%MZ",    // Ad-hoc variants
      "%F %H:%MZ",    // ...
  };

  // Unix time (no leap seconds)
  date::sys_time<Duration> tp;
  std::string unused_abbrev;
  std::chrono::minutes offset;

  int attempt               = 0;
  constexpr int num_formats = formats.size();
  int idx                   = last_format_;
  for (; attempt < num_formats; ++attempt) {
    idx = last_format_ + attempt;
    if (idx >= num_formats) {
      idx -= num_formats;
    }

    // Ugh, string_view streams one day.
    std::stringstream in{str};
    in >> date::parse(formats.at(idx), tp, unused_abbrev, offset);
    if (!in) {
      continue;
    }

    using value_type = std::decay_t<decltype(str)>::value_type;

    if (in.peek() == std::char_traits<value_type>::eof()) {
      // Parsed using the entire string
      break;
    }
  }

  if (attempt >= num_formats) {
    return std::nullopt;
  }

  last_format_ = idx;

  return std::chrono::duration_cast<Duration>(tp.time_since_epoch()).count();
}

#endif

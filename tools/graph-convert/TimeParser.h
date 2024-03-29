#ifndef KATANA_TOOLS_GRAPHCONVERT_TIMEPARSER_H_
#define KATANA_TOOLS_GRAPHCONVERT_TIMEPARSER_H_

#include <array>
#include <chrono>
#include <optional>
#include <sstream>

#include <arrow/api.h>
#include <arrow/vendored/datetime/date.h>

#include "katana/Logging.h"

namespace date = arrow_vendored::date;

namespace katana {

/// A TimeParser parses various string formats into a Unix timestamp.
///
/// \tparam Duration a std::chrono::duration indicating the resolution/units of
/// the returned timestamps
template <class ArrowDateTimeType, typename Duration>
class TimeParser {
  using CType = typename arrow::TypeTraits<ArrowDateTimeType>::CType;
  using BuilderType =
      typename arrow::TypeTraits<ArrowDateTimeType>::BuilderType;
  int last_format_{0};

public:
  /// Parse parses a string and returns a Unix timestamp in units of Duration.
  /// If the string could not be parsed, the null option is returned.
  std::optional<CType> Parse(const std::string& str);
  /// calls Parse on each string in the StringArray, appending into the builder
  void ParseInto(
      const arrow::StringArray& strings, arrow::ArrayBuilder* builder);
};

}  // namespace katana

template <class ArrowDateTimeType, typename Duration>
std::optional<typename katana::TimeParser<ArrowDateTimeType, Duration>::CType>
katana::TimeParser<ArrowDateTimeType, Duration>::Parse(const std::string& str) {
  if (str.empty()) {
    return std::nullopt;
  }
  // Possible time formats
  //
  // ISO 8601:
  //  2020-11-22T11:22:33.52Z or
  //  2020-11-22 11:22:33.52Z
  //
  // RFC 3339:
  //  2020-11-22 11:22:33.52Z only
  std::array formats{
      "%F %T%Z",     // RFC 3339 UTC
      "%FT%T%Z",     // ISO 8601 UTC
      "%FT%H:%M%Z",  // Ad-hoc variants
      "%F %H:%M%Z",  // ...
      "%F",          // Date
  };

  // Due to a bug in the date library these values should be initialized to 0.
  // The parser does not check for the presence of specific "formats" (e.g., %z)
  // in the format string before using the values they parse. This definitely
  // affects tz_offset, but it might affect others.

  // Unix time (no leap seconds)
  date::sys_time<Duration> tp{};
  // Time zone abbrevation (if %Z in format string)
  std::string tz_abbrev;
  // Time zone offset (if %z in format string)
  std::chrono::minutes tz_offset{0};

  int attempt = 0;
  constexpr int num_formats = formats.size();
  int idx = last_format_;
  for (; attempt < num_formats; ++attempt) {
    idx = last_format_ + attempt;
    if (idx >= num_formats) {
      idx -= num_formats;
    }

    // Ugh, string_view streams one day.
    std::stringstream in{str};
    in >> date::parse(formats.at(idx), tp, tz_abbrev, tz_offset);

    if (!in) {
      continue;
    }

    if (in.peek(); in.eof()) {
      if (!tz_abbrev.empty() && tz_abbrev != "Z") {
        KATANA_LOG_WARN(
            "datetime string ({}) references unsupported timezone ({})", str,
            tz_abbrev);
      }
      if (tz_offset != std::chrono::minutes{0}) {
        KATANA_LOG_WARN(
            "datetime string ({}) references unsupported offset ({})", str,
            tz_offset.count());
      }
      last_format_ = idx;
      return tp.time_since_epoch().count();
    } else {
      KATANA_LOG_DEBUG(
          "incomplete parsing of ({}) using ({}), trying other formats", str,
          formats.at(idx));
    }
  }

  KATANA_LOG_WARN("could not parse datetime string ({})", str);

  return std::nullopt;
}

template <class ArrowDateTimeType, typename Duration>
void
katana::TimeParser<ArrowDateTimeType, Duration>::ParseInto(
    const arrow::StringArray& strings, arrow::ArrayBuilder* untyped_builder) {
  BuilderType& builder = dynamic_cast<BuilderType&>(*untyped_builder);
  if (auto st = builder.Reserve(strings.length()); !st.ok()) {
    KATANA_LOG_FATAL("builder failed to reserve space");
  }
  for (size_t i = 0, n = strings.length(); i < n; ++i) {
    std::string str = strings.GetString(i);
    auto r = Parse(str);
    if (r) {
      builder.UnsafeAppend(*r);
    } else {
      builder.UnsafeAppendNull();
    }
  }
}

#endif

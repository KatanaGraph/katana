#include "katana/Result.h"

#include <sstream>
#include <type_traits>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Strings.h"

namespace {
void
TestConversions() {
  static_assert(std::is_convertible_v<katana::ErrorCode, std::error_code>);

  static_assert(
      !std::is_convertible_v<katana::ErrorInfo, std::error_code>,
      "Do not allow implicit conversion from ErrorInfo to error_code becase it "
      "is lossy");

  // Check error_code to error_condition
  std::error_code not_found = katana::ErrorCode::NotFound;

  KATANA_LOG_VASSERT(
      not_found == std::errc::no_such_file_or_directory,
      "expected custom error code to be convertable to std error condition");
}

std::string
ToString(const katana::ErrorInfo& ei) {
  std::stringstream out;
  out << ei;
  return out.str();
}

void
TestMessages() {
  // Check printing
  katana::Result<void> res = katana::ErrorCode::NotFound;

  auto err = res.error();

  std::string found = ToString(err);
  KATANA_LOG_VASSERT(
      found == "not found", "expected string 'not found' but found: {}", found);

  err = err.WithContext("1");
  found = ToString(err);
  KATANA_LOG_VASSERT(found == "1", "expected string '1' but found: {}", found);

  err = err.WithContext("2");
  found = ToString(err);
  KATANA_LOG_VASSERT(
      found == "2: 1", "expected string '2: 1' but found: {}", found);

  std::string long_string(2 * katana::ErrorInfo::kContextSize, 'x');
  long_string += "sentinel";
  err = err.WithContext(long_string);
  found = ToString(err);
  KATANA_LOG_VASSERT(
      katana::HasSuffix(found, "sentinel: 2: 1"),
      "expected string suffix 'sentinel: 2: 1' but found: {}", found);
}

void
TestFmt() {
  katana::Result<void> res = katana::ErrorCode::NotFound;

  auto err = res.error();

  std::string found = ToString(err);

  fmt::memory_buffer fbuf;
  fmt::format_to(fbuf, "{}", err);
  std::string fstr(fbuf.begin(), fbuf.end());
  KATANA_LOG_VASSERT(
      fstr == found,
      "stream and fmt should return the same result but found {} and {}", found,
      fstr);
}

}  // namespace

int
main() {
  TestConversions();
  TestMessages();
  TestFmt();
}

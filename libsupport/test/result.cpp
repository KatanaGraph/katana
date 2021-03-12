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
  katana::ErrorInfo err(katana::ErrorCode::NotFound, "0");

  err = err.WithContext("1");
  std::string found = ToString(err);
  KATANA_LOG_VASSERT(
      found == "1: 0", "expected string '1: 0' but found: {}", found);

  err = err.WithContext("2");
  found = ToString(err);
  KATANA_LOG_VASSERT(
      found == "2: 1: 0", "expected string '2: 1: 0' but found: {}", found);

  std::string long_string(2 * katana::ErrorInfo::kContextSize, 'x');
  long_string += "sentinel";
  err = err.WithContext(long_string);
  found = ToString(err);
  KATANA_LOG_VASSERT(
      katana::HasSuffix(found, "sentinel: 2: 1: 0"),
      "expected string suffix 'sentinel: 2: 1: 0' but found: {}", found);
}

void
TestResetBetweenInstances() {
  katana::ErrorInfo err1(katana::ErrorCode::NotFound, "1");
  err1 = err1.WithContext("one");
  std::string found1 = ToString(err1);

  katana::ErrorInfo err2(katana::ErrorCode::NotFound, "2");
  err2 = err2.WithContext("two");
  std::string found2 = ToString(err2);

  KATANA_LOG_VASSERT(
      found1 == "one: 1", "expected 'one: 1' but found {}", found1);
  KATANA_LOG_VASSERT(
      found2 == "two: 2", "expected 'two: 2' but found {}", found1);
}

void
TestContextSpill() {
  katana::ErrorInfo err = katana::ErrorCode::NotFound;
  err = err.WithContext("more");
  std::string found = ToString(err);

  std::error_code ec = katana::ErrorCode::NotFound;
  std::string expected = "more: " + ec.message();

  KATANA_LOG_VASSERT(
      found == expected, "expected {} but found {}", expected, found);
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
  TestResetBetweenInstances();
  TestContextSpill();
}

#include "TimeParser.h"

void
TestBasic() {
  katana::TimeParser<arrow::TimestampType, std::chrono::seconds> parser;

  {
    auto r = parser.Parse("1970-01-01T00:00:01Z");
    KATANA_LOG_VASSERT(*r == 1, "*r == {}", *r);
  }
  {
    auto r = parser.Parse("1970-01-01 00:00:01Z");
    KATANA_LOG_VASSERT(*r == 1, "*r == {}", *r);
  }
  {
    auto r = parser.Parse("1970-01-01 00:01Z");
    KATANA_LOG_VASSERT(*r == 60, "*r == {}", *r);
  }
}

int
main() {
  TestBasic();
  return 0;
}

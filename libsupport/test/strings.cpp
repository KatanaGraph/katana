#include "katana/Strings.h"

#include <list>

#include "katana/Logging.h"

int
main() {
  KATANA_LOG_ASSERT(katana::HasSuffix("prefix.suffix", ".suffix"));
  KATANA_LOG_ASSERT(katana::HasSuffix("prefix.suffix", ""));
  KATANA_LOG_ASSERT(!katana::HasSuffix("prefix.suffix", "none"));
  KATANA_LOG_ASSERT(!katana::HasSuffix("", "none"));
  KATANA_LOG_ASSERT(katana::TrimSuffix("prefix.suffix", ".suffix") == "prefix");
  KATANA_LOG_ASSERT(
      katana::TrimSuffix("prefix.suffix", "none") == "prefix.suffix");

  KATANA_LOG_ASSERT(katana::HasPrefix("prefix.suffix", "prefix."));
  KATANA_LOG_ASSERT(katana::HasPrefix("prefix.suffix", ""));
  KATANA_LOG_ASSERT(!katana::HasPrefix("prefix.suffix", "none"));
  KATANA_LOG_ASSERT(!katana::HasPrefix("", "none"));
  KATANA_LOG_ASSERT(katana::TrimPrefix("prefix.suffix", "prefix.") == "suffix");
  KATANA_LOG_ASSERT(
      katana::TrimSuffix("prefix.suffix", "none") == "prefix.suffix");

  KATANA_LOG_ASSERT(
      katana::Join(" ", {"list", "of", "strings"}) == "list of strings");
  KATANA_LOG_ASSERT(
      katana::Join("", {"list", "of", "strings"}) == "listofstrings");
  KATANA_LOG_ASSERT(katana::Join(" ", {"string"}) == "string");
  KATANA_LOG_ASSERT(katana::Join(" ", std::vector<std::string>{}).empty());
  KATANA_LOG_ASSERT(
      katana::Join(" ", {"list", "of", "", "strings"}) == "list of  strings");

  KATANA_LOG_ASSERT(katana::Join(" ", std::list<int>{1, 2, 3}) == "1 2 3");
}

#include "galois/Strings.h"

#include <list>

#include "galois/Logging.h"

int
main() {
  GALOIS_LOG_ASSERT(galois::HasSuffix("prefix.suffix", ".suffix"));
  GALOIS_LOG_ASSERT(galois::HasSuffix("prefix.suffix", ""));
  GALOIS_LOG_ASSERT(!galois::HasSuffix("prefix.suffix", "none"));
  GALOIS_LOG_ASSERT(!galois::HasSuffix("", "none"));
  GALOIS_LOG_ASSERT(galois::TrimSuffix("prefix.suffix", ".suffix") == "prefix");
  GALOIS_LOG_ASSERT(
      galois::TrimSuffix("prefix.suffix", "none") == "prefix.suffix");

  GALOIS_LOG_ASSERT(galois::HasPrefix("prefix.suffix", "prefix."));
  GALOIS_LOG_ASSERT(galois::HasPrefix("prefix.suffix", ""));
  GALOIS_LOG_ASSERT(!galois::HasPrefix("prefix.suffix", "none"));
  GALOIS_LOG_ASSERT(!galois::HasPrefix("", "none"));
  GALOIS_LOG_ASSERT(galois::TrimPrefix("prefix.suffix", "prefix.") == "suffix");
  GALOIS_LOG_ASSERT(
      galois::TrimSuffix("prefix.suffix", "none") == "prefix.suffix");

  GALOIS_LOG_ASSERT(
      galois::Join(" ", {"list", "of", "strings"}) == "list of strings");
  GALOIS_LOG_ASSERT(
      galois::Join("", {"list", "of", "strings"}) == "listofstrings");
  GALOIS_LOG_ASSERT(galois::Join(" ", {"string"}) == "string");
  GALOIS_LOG_ASSERT(galois::Join(" ", std::vector<std::string>{}).empty());
  GALOIS_LOG_ASSERT(
      galois::Join(" ", {"list", "of", "", "strings"}) == "list of  strings");

  GALOIS_LOG_ASSERT(galois::Join(" ", std::list<int>{1, 2, 3}) == "1 2 3");
}

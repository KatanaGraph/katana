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
      katana::SplitView("separated by spaces", " ") ==
      std::vector<std::string_view>({"separated", "by", "spaces"}));
  KATANA_LOG_ASSERT(
      katana::SplitView("no delimiter in string", ";") ==
      std::vector<std::string_view>({"no delimiter in string"}));
  KATANA_LOG_ASSERT(
      katana::SplitView("", " ") == std::vector<std::string_view>({""}));
  KATANA_LOG_ASSERT(
      katana::SplitView(",delim,corner,,cases,", ",") ==
      std::vector<std::string_view>({"", "delim", "corner", "", "cases", ""}));
  KATANA_LOG_ASSERT(
      katana::SplitView("what if word delim word is a word word", " word ") ==
      std::vector<std::string_view>({"what if", "delim", "is a", "word"}));
  KATANA_LOG_ASSERT(
      katana::SplitView("empty", "") ==
      std::vector<std::string_view>({"e", "m", "p", "t", "y"}));
  KATANA_LOG_ASSERT(
      katana::SplitView("only\tsplit\tonce", "\t", 1) ==
      std::vector<std::string_view>({"only", "split\tonce"}));
  KATANA_LOG_ASSERT(
      katana::SplitView("split\tthe\tright\tamount", "\t", 3) ==
      std::vector<std::string_view>({"split", "the", "right", "amount"}));

  KATANA_LOG_ASSERT(
      katana::Join(" ", {"list", "of", "strings"}) == "list of strings");
  KATANA_LOG_ASSERT(
      katana::Join("", {"list", "of", "strings"}) == "listofstrings");
  KATANA_LOG_ASSERT(katana::Join(" ", {"string"}) == "string");
  KATANA_LOG_ASSERT(katana::Join(" ", std::vector<std::string>{}).empty());
  KATANA_LOG_ASSERT(
      katana::Join(" ", {"list", "of", "", "strings"}) == "list of  strings");

  KATANA_LOG_ASSERT(katana::Join(" ", std::list<int>{1, 2, 3}) == "1 2 3");

  KATANA_LOG_ASSERT(katana::ToBase64("") == "");
  KATANA_LOG_ASSERT(katana::ToBase64("uchigatana") == "dWNoaWdhdGFuYQ==");
  KATANA_LOG_ASSERT(katana::ToBase64("tachi") == "dGFjaGk=");
  KATANA_LOG_ASSERT(katana::ToBase64("katana") == "a2F0YW5h");
  KATANA_LOG_ASSERT(
      katana::ToBase64(std::string("\xFF\xFF\xFF"), true) == "____");
  KATANA_LOG_ASSERT(katana::FromBase64("") == "");
  KATANA_LOG_ASSERT(katana::FromBase64("dGFjaGk=") == "tachi");
  KATANA_LOG_ASSERT(katana::FromBase64("dWNoaWdhdGFuYQ==") == "uchigatana");
  KATANA_LOG_ASSERT(katana::FromBase64("a2F0YW5h") == "katana");
}

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "katana/ExtraTraits.h"

void
TestIsMapTraits() {
  static_assert(
      katana::is_map<std::unordered_map<std::string, uint64_t>>::value);
  static_assert(katana::is_map<std::map<std::string, uint64_t>>::value);
  static_assert(!katana::is_map<std::unordered_set<std::string>>::value);
  static_assert(!katana::is_map<std::set<std::string>>::value);
}

int
main() {
  TestIsMapTraits();
  return 0;
}

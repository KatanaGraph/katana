#include "katana/Range.h"

struct LocalContainer {
  using iterator = int*;
  using local_iterator = int*;
  using value_type = int;

  iterator begin();
  iterator end();
  local_iterator local_begin();
  local_iterator local_end();
};

struct StandardContainer {
  using iterator = int*;
  using value_type = int;

  iterator begin();
  iterator end();
};

namespace {

template <typename T>
constexpr std::true_type
IsLocalRange(katana::LocalRange<T>) {
  return std::true_type();
}

template <typename T>
constexpr std::false_type
IsLocalRange(T) {
  return std::false_type();
}

void
TestLocal() {
  LocalContainer local{};
  StandardContainer standard{};

  static_assert(katana::has_local_iterator_v<LocalContainer>);
  static_assert(!katana::has_local_iterator_v<StandardContainer>);

  static_assert(decltype(IsLocalRange(katana::iterate(local)))::value);
  static_assert(!decltype(IsLocalRange(katana::iterate(standard)))::value);
}

void
TestBlockRange(const char* name, int begin, int end, int num) {
  std::vector<int> counts;
  counts.resize(end);

  for (int i = 0; i < num; ++i) {
    auto r = katana::block_range(begin, end, i, num);
    for (; r.first != r.second; ++r.first) {
      KATANA_LOG_VASSERT(r.first < end, "{} < {}", r.first, end);
      KATANA_LOG_VASSERT(r.first >= begin, "{} >= {}", r.first, begin);
      counts[r.first] += 1;
    }
  }

  for (int idx = begin; idx < end; ++idx) {
    auto v = counts[idx];
    KATANA_LOG_VASSERT(
        v == 1, "{}: index {}: expected {} found {}", name, idx, 1, v);
  }
}

}  // namespace

int
main() {
  TestLocal();

  TestBlockRange("empty", 0, 0, 1);
  TestBlockRange("zero", 0, 0, 0);
  TestBlockRange("large block", 0, 4, 10);
  TestBlockRange("uneven", 0, 10, 4);
  TestBlockRange("even", 0, 10, 5);
  TestBlockRange("very uneven", 0, 21, 10);

  TestBlockRange("non-zero begin: empty", 1, 1, 1);
  TestBlockRange("non-zero begin: zero", 1, 0, 0);
  TestBlockRange("non-zero begin: large block", 1, 5, 10);
  TestBlockRange("non-zero begin: uneven", 1, 11, 4);
  TestBlockRange("non-zero begin: even", 1, 11, 5);
  TestBlockRange("non-zero begin: very uneven", 1, 22, 10);

  return 0;
}

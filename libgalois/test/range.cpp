#include "galois/Range.h"

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

template <typename T>
constexpr std::true_type
IsLocalRange(galois::LocalRange<T>) {
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

  static_assert(galois::has_local_iterator_v<LocalContainer>);
  static_assert(!galois::has_local_iterator_v<StandardContainer>);

  static_assert(decltype(IsLocalRange(galois::iterate(local)))::value);
  static_assert(!decltype(IsLocalRange(galois::iterate(standard)))::value);
}

int
main() {
  TestLocal();
  return 0;
}

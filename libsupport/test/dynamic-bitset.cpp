#include "katana/DynamicBitsetSlow.h"

int
main() {
  katana::DynamicBitsetSlow bs1;
  bs1.resize(100);
  // Test some simple sets, resets, and tests
  for (int i : {40, 3, 5, 10}) {
    KATANA_LOG_ASSERT(!bs1.test(i));
    bs1.set(i);
    KATANA_LOG_ASSERT(bs1.test(i));

    katana::DynamicBitsetSlow bs2;
    bs2.resize(i + 1);
    KATANA_LOG_ASSERT(!bs2.test(i));
    bs2.set(i);
    KATANA_LOG_ASSERT(bs2.test(i));
    bs2.reset(i);
    KATANA_LOG_ASSERT(!bs2.test(i));
  }

  KATANA_LOG_ASSERT(!bs1.test(2));
  KATANA_LOG_ASSERT(!bs1.test(11));

  auto b = bs1.begin();
  auto e = bs1.end();
  auto e1 = ++(++(++(++bs1.begin())));
  KATANA_LOG_ASSERT(e == e1);
  KATANA_LOG_ASSERT(b != e1);

  // Test the iterator
  int count = 0;
  for (auto i : bs1) {
    KATANA_LOG_VASSERT(bs1.test(i), "{} not set", i);
    count++;
  }

  KATANA_LOG_ASSERT(count == 4);

  std::vector<uint64_t> ones;
  ones.resize(count);
  std::copy(bs1.begin(), bs1.end(), ones.begin());

  KATANA_LOG_ASSERT(ones[0] == 3);
  KATANA_LOG_ASSERT(ones[1] == 5);
  KATANA_LOG_ASSERT(ones[2] == 10);
  KATANA_LOG_ASSERT(ones[3] == 40);

  // Test the global reset because it's easy
  KATANA_LOG_ASSERT(bs1.test(10));
  bs1.reset();
  KATANA_LOG_ASSERT(!bs1.test(10));

  return 0;
}

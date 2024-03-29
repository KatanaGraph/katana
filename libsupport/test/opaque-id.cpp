#include "katana/Logging.h"
#include "katana/OpaqueID.h"

// Make sure the OpaqueIDs don't have memory overhead.

struct TestCharUnordered : public katana::OpaqueID<TestCharUnordered, char> {
  using OpaqueID::OpaqueID;
};
static_assert(sizeof(TestCharUnordered) == sizeof(char));
static_assert(alignof(TestCharUnordered) == alignof(char));

struct TestCharOrdered : public katana::OpaqueIDLinear<TestCharOrdered, char> {
  using OpaqueIDLinear::OpaqueIDLinear;
};
static_assert(sizeof(TestCharOrdered) == sizeof(char));
static_assert(alignof(TestCharOrdered) == alignof(char));

struct TestLongUnordered : public katana::OpaqueID<TestLongUnordered, long> {
  using OpaqueID::OpaqueID;
};
static_assert(sizeof(TestLongUnordered) == sizeof(long));
static_assert(alignof(TestLongUnordered) == alignof(long));

struct TestLongOrdered : public katana::OpaqueIDLinear<TestLongOrdered, long> {
  using OpaqueIDLinear::OpaqueIDLinear;
};
static_assert(sizeof(TestLongOrdered) == sizeof(long));
static_assert(alignof(TestLongOrdered) == alignof(long));

// Make sure sentinel is okay
static_assert(
    std::is_same_v<decltype(TestCharOrdered::sentinel()), TestCharOrdered>);
static_assert(
    std::is_same_v<decltype(TestLongOrdered::sentinel()), TestLongOrdered>);

struct IntID : public katana::OpaqueIDLinear<IntID, int> {
  using OpaqueIDLinear::OpaqueIDLinear;
};

void
TestPrint() {
  int value = 1;
  IntID id(value);

  std::stringstream expected;
  expected << value;

  fmt::memory_buffer fmt_buf;
  fmt::format_to(std::back_inserter(fmt_buf), "{}", id);

  std::stringstream stl_buf;
  stl_buf << id;

  KATANA_LOG_ASSERT(to_string(fmt_buf) == expected.str());
  KATANA_LOG_ASSERT(stl_buf.str() == expected.str());
}

int
main() {
  TestPrint();

  return 0;
}

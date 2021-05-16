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

int
main() {
  return 0;
}

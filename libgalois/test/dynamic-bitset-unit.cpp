#include "katana/DynamicBitset.h"
#include "katana/Result.h"

namespace {
using TestCaseGenerator = std::function<katana::DynamicBitset()>;
using Invariant = std::function<katana::Result<void>(katana::DynamicBitset*)>;

const TestCaseGenerator TestBitsetEmpty = []() {
  katana::DynamicBitset test;
  return test;
};

// shorter than the size of one betvec entry
const TestCaseGenerator TestBitsetOne = []() {
  katana::DynamicBitset test;
  test.resize(32);
  test.reset();

  for (size_t i = 0; i < test.size(); i += 3) {
    test.set(i);
  }

  return test;
};

// longer than the size of one betvec entry
const TestCaseGenerator TestBitsetTwo = []() {
  katana::DynamicBitset test;
  test.resize(74);
  test.reset();

  for (size_t i = 0; i < test.size(); i += 3) {
    test.set(i);
  }

  return test;
};

// only set first and last
const TestCaseGenerator TestBitsetThree = []() {
  katana::DynamicBitset test;
  test.resize(74);
  test.reset();

  test.set(0);
  test.set(73);

  return test;
};

// big
const TestCaseGenerator TestBitsetFour = []() {
  katana::DynamicBitset test;
  test.resize(12345);
  test.reset();

  for (size_t i = 0; i < test.size(); i += 3) {
    test.set(i);
  }

  return test;
};

// exactly one bitvec entry
const TestCaseGenerator TestBitsetFive = []() {
  katana::DynamicBitset test;
  test.resize(64);
  test.reset();

  for (size_t i = 0; i < test.size(); i += 3) {
    test.set(i);
  }

  return test;
};

const std::vector<TestCaseGenerator> test_case_generators = {
    TestBitsetEmpty, TestBitsetOne,  TestBitsetTwo,
    TestBitsetThree, TestBitsetFour, TestBitsetFive};

const Invariant NotAndCount =
    [](katana::DynamicBitset* test) -> katana::Result<void> {
  size_t size = test->size();
  size_t count_before = test->count();
  test->bitwise_not();
  size_t count_after = test->count();

  if (size != count_before + count_after) {
    return KATANA_ERROR(
        katana::ErrorCode::AssertionFailed,
        "count of bitset and count of complement did not sum to size of "
        "bitset "
        "- size of bitset: {}, count of bitset: {}, count of complement: {}",
        size, count_before, count_after);
  }

  return katana::ResultSuccess();
};

const Invariant NotValues =
    [](katana::DynamicBitset* test) -> katana::Result<void> {
  katana::DynamicBitset other;
  other.resize(test->size());
  other.reset();
  for (size_t i = 0, size = test->size(); i < size; ++i) {
    if (test->test(i)) {
      other.set(i);
    }
  }

  other.bitwise_not();

  for (size_t i = 0, size = test->size(); i < size; ++i) {
    if (test->test(i)) {
      if (other.test(i)) {
        return KATANA_ERROR(
            katana::ErrorCode::AssertionFailed,
            "bitwise_not failed to invert a bit - bit {} is set in original "
            "bitset and also in notted bitset",
            i);
      }
    } else {
      if (!other.test(i)) {
        return KATANA_ERROR(
            katana::ErrorCode::AssertionFailed,
            "bitwise_not failed to invert a bit - bit {} is not set in "
            "original bitset and also not set in notted bitset",
            i);
      }
    }
  }

  return katana::ResultSuccess();
};

const std::vector<Invariant> invariants = {NotAndCount, NotValues};

katana::Result<void>
TestAll() {
  for (const auto& generator : test_case_generators) {
    for (const auto& invariant : invariants) {
      auto bitset = generator();
      KATANA_CHECKED(invariant(&bitset));
    }
  }

  return katana::ResultSuccess();
}

}  // namespace

int
main() {
  katana::GaloisRuntime Katana_runtime;

  auto res = TestAll();
  KATANA_LOG_VASSERT(res, "{}", res.error());
  return 0;
}

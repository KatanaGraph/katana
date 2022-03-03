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
CountAndOffsetsTest() {
  int num_threads = 4;
  katana::setActiveThreads(num_threads);

  std::vector<uint64_t> set_bits;
  katana::DynamicBitset test_bitset;
  test_bitset.resize(130);

  //////////////////////////////////////////////////////////////////////////////
  // single bit
  //////////////////////////////////////////////////////////////////////////////

  test_bitset.set(64);
  KATANA_LOG_ASSERT(test_bitset.test(64));

  KATANA_LOG_ASSERT(test_bitset.count() == 1);
  KATANA_LOG_ASSERT(test_bitset.SerialCount() == 1);

  set_bits = test_bitset.GetOffsets<uint64_t>();
  KATANA_LOG_ASSERT(set_bits.size() == 1);
  KATANA_LOG_ASSERT(set_bits[0] == 64);
  set_bits = test_bitset.GetOffsetsSerial<uint64_t>();
  KATANA_LOG_ASSERT(set_bits.size() == 1);
  KATANA_LOG_ASSERT(set_bits[0] == 64);

  //////////////////////////////////////////////////////////////////////////////
  // two bits
  //////////////////////////////////////////////////////////////////////////////

  test_bitset.set(63);
  KATANA_LOG_ASSERT(test_bitset.test(63));

  KATANA_LOG_ASSERT(test_bitset.count() == 2);
  KATANA_LOG_ASSERT(test_bitset.SerialCount() == 2);

  // offsets are going to be returned ordered
  set_bits = test_bitset.GetOffsets<uint64_t>();
  KATANA_LOG_ASSERT(set_bits.size() == 2);
  KATANA_LOG_ASSERT(set_bits[0] == 63);
  KATANA_LOG_ASSERT(set_bits[1] == 64);
  set_bits = test_bitset.GetOffsetsSerial<uint64_t>();
  KATANA_LOG_ASSERT(set_bits.size() == 2);
  KATANA_LOG_ASSERT(set_bits[0] == 63);
  KATANA_LOG_ASSERT(set_bits[1] == 64);

  //////////////////////////////////////////////////////////////////////////////
  // three bits
  //////////////////////////////////////////////////////////////////////////////

  test_bitset.set(129);
  KATANA_LOG_ASSERT(test_bitset.test(129));

  KATANA_LOG_ASSERT(test_bitset.count() == 3);
  KATANA_LOG_ASSERT(test_bitset.SerialCount() == 3);

  set_bits = test_bitset.GetOffsets<uint64_t>();
  KATANA_LOG_ASSERT(set_bits.size() == 3);
  KATANA_LOG_ASSERT(set_bits[0] == 63);
  KATANA_LOG_ASSERT(set_bits[1] == 64);
  KATANA_LOG_ASSERT(set_bits[2] == 129);
  set_bits = test_bitset.GetOffsetsSerial<uint64_t>();
  KATANA_LOG_ASSERT(set_bits.size() == 3);
  KATANA_LOG_ASSERT(set_bits[0] == 63);
  KATANA_LOG_ASSERT(set_bits[1] == 64);
  KATANA_LOG_ASSERT(set_bits[2] == 129);

  //////////////////////////////////////////////////////////////////////////////
  // one full int and 2 bits
  //////////////////////////////////////////////////////////////////////////////

  for (size_t i = 0; i < 64; i++) {
    test_bitset.set(i);
  }
  katana::do_all(katana::iterate(0, 64), [&](auto i) {
    KATANA_LOG_ASSERT(test_bitset.test(i));
  });

  KATANA_LOG_ASSERT(test_bitset.count() == 66);
  KATANA_LOG_ASSERT(test_bitset.SerialCount() == 66);

  set_bits = test_bitset.GetOffsets<uint64_t>();
  KATANA_LOG_ASSERT(set_bits.size() == 66);
  for (size_t i = 0; i < 64; i++) {
    KATANA_LOG_ASSERT(set_bits[i] == i);
  }
  KATANA_LOG_ASSERT(set_bits[64] == 64);
  KATANA_LOG_ASSERT(set_bits[65] == 129);

  set_bits = test_bitset.GetOffsetsSerial<uint64_t>();
  KATANA_LOG_ASSERT(set_bits.size() == 66);
  for (size_t i = 0; i < 64; i++) {
    KATANA_LOG_ASSERT(set_bits[i] == i);
  }
  KATANA_LOG_ASSERT(set_bits[64] == 64);
  KATANA_LOG_ASSERT(set_bits[65] == 129);

  return katana::ResultSuccess();
}

katana::Result<void>
TestAll() {
  for (const auto& generator : test_case_generators) {
    for (const auto& invariant : invariants) {
      auto bitset = generator();
      KATANA_CHECKED(invariant(&bitset));
    }
  }

  KATANA_CHECKED(CountAndOffsetsTest());

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

#include "katana/DynamicBitset.h"
#include "katana/Result.h"

namespace {
using TestCaseGenerator = std::function<katana::DynamicBitset()>;
using Invariant = std::function<katana::Result<void>(katana::DynamicBitset*)>;

const TestCaseGenerator TestBitsetEmpty = []() {
  katana::DynamicBitset test;
  return test;
};

const TestCaseGenerator TestBitsetOne = []() {
  katana::DynamicBitset test;
  test.resize(32);
  test.reset();

  for (size_t i = 0; i < test.size(); i += 3) {
    test.set(i);
  }

  return test;
};

const std::vector<TestCaseGenerator> test_case_generators = {
    TestBitsetEmpty, TestBitsetOne};

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

const std::vector<Invariant> invariants = {NotAndCount};

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

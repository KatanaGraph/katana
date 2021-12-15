#include "katana/Experimental.h"

#include <algorithm>

#include "katana/Logging.h"

KATANA_EXPERIMENTAL_FEATURE(TestOn);
KATANA_EXPERIMENTAL_FEATURE(TestOff);
KATANA_EXPERIMENTAL_FEATURE(TestSecond);

KATANA_EXPERIMENTAL_FEATURE(DefinedButUnused);

// this causes the compiler to print:
//   error: static assertion failed: KATANA_EXPERIMENTAL_FEATURE must not be
//      inside a namespace block
//
// namespace test {
//
// KATANA_EXPERIMENTAL_FEATURE(ShouldNotCompile);
//
// } // namespace test

int
main() {
  KATANA_LOG_ASSERT(KATANA_EXPERIMENTAL_ENABLED(TestOn));
  KATANA_LOG_ASSERT(!KATANA_EXPERIMENTAL_ENABLED(TestOff));
  KATANA_LOG_ASSERT(KATANA_EXPERIMENTAL_ENABLED(TestSecond));

  auto unused_in_env =
      katana::internal::ExperimentalFeature::ReportUnrecognized();
  KATANA_LOG_ASSERT(unused_in_env.size() == 1);
  KATANA_LOG_ASSERT(unused_in_env[0] == "EnvironmentOnly");

  auto enabled = katana::internal::ExperimentalFeature::ReportEnabled();
  KATANA_LOG_ASSERT(enabled.size() == 3);
  std::sort(enabled.begin(), enabled.end());
  KATANA_LOG_ASSERT(enabled[0] == "DefinedButUnused");
  KATANA_LOG_ASSERT(enabled[1] == "TestOn");
  KATANA_LOG_ASSERT(enabled[2] == "TestSecond");

  auto disabled = katana::internal::ExperimentalFeature::ReportDisabled();
  KATANA_LOG_ASSERT(disabled.size() == 1);
  KATANA_LOG_ASSERT(disabled[0] == "TestOff");
}

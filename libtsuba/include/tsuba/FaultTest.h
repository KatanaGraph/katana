#ifndef KATANA_LIBTSUBA_TSUBA_FAULTTEST_H_
#define KATANA_LIBTSUBA_TSUBA_FAULTTEST_H_

#include <cstdint>
#include <unordered_map>

#include "katana/config.h"

namespace tsuba::internal {

enum class FaultSensitivity { Normal, High };
enum class FaultMode {
  None,            // No faults
  Independent,     // Each point has a fixed probability of failure
  RunLength,       // Specify the number call on which to crash (starts at 1)
  UniformOverRun,  // Choose uniform run length 1..run_length (exclusive)
};

KATANA_EXPORT void FaultTestInit(
    FaultMode mode = FaultMode::None, float independent_prob = 0.0f,
    uint64_t run_length = UINT64_C(0));
// LOG_VERBOSE stats
KATANA_EXPORT void FaultTestReport();

// PullThePlug (virtually) Compile this out if NDEBUG?
#define TSUBA_PTP(...)                                                         \
  do {                                                                         \
    ::tsuba::internal::PtP(__FILE__, __LINE__, ##__VA_ARGS__);                 \
  } while (0)

KATANA_EXPORT void PtP(
    const char* file, int line,
    FaultSensitivity sensitivity = FaultSensitivity::Normal);

}  // namespace tsuba::internal

#endif

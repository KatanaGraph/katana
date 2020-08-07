#ifndef GALOIS_LIBTSUBA_TSUBA_FAULTTEST_H_
#define GALOIS_LIBTSUBA_TSUBA_FAULTTEST_H_

#include <cstdint>
#include <unordered_map>

#include "galois/config.h"

namespace tsuba::internal {

enum class FaultSensitivity { Normal, High };
enum class FaultMode {
  None,           // No faults
  Independent,    // Each point has a fixed probability of failure
  RunLength,      // Specify the number call on which to crash (starts at 1)
  UniformOverRun, // Choose uniform run length 1..run_length (exclusive)
};

GALOIS_EXPORT void FaultTestInit(FaultMode mode         = FaultMode::None,
                                 float independent_prob = 0.0f,
                                 uint64_t run_length    = 0UL);
// LOG_VERBOSE stats
GALOIS_EXPORT void FaultTestReport();

// PullThePlug (virtually) Compile this out if NDEBUG?
void PtP(FaultSensitivity sensitivity = FaultSensitivity::Normal);

} // namespace tsuba::internal

#endif

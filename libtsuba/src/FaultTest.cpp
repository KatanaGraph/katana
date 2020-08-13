// FaultTest: support for injecting faults into Katana's storage layer in order
//  to test our crash recovery and transaction implementation

#include "tsuba/FaultTest.h"
#include "galois/Random.h"
#include "galois/Logging.h"

static tsuba::internal::FaultMode mode_{tsuba::internal::FaultMode::None};
static float independent_prob_{0.0f};
static uint64_t run_length_{0UL};
static uint64_t fault_run_length_{0UL};
static uint64_t ptp_count_{0UL};
static const std::unordered_map<tsuba::internal::FaultMode, std::string>
    fault_mode_label{
        {tsuba::internal::FaultMode::None, "No faults"},
        {tsuba::internal::FaultMode::Independent, "Independent"},
        {tsuba::internal::FaultMode::RunLength, "RunLength"},
        {tsuba::internal::FaultMode::UniformOverRun, "UniformOverRun"},
    };

void tsuba::internal::FaultTestReport() {
  fmt::print("PtP count: {:d}\n", ptp_count_);
}

void tsuba::internal::FaultTestInit(tsuba::internal::FaultMode mode,
                                    float independent_prob,
                                    uint64_t run_length) {
  mode_             = mode;
  independent_prob_ = independent_prob;
  run_length_       = run_length;
  // Configuration sanity check
  if (run_length > (1UL << 40)) {
    GALOIS_LOG_WARN("Large run length {:d}", run_length);
  }
  GALOIS_LOG_VASSERT(independent_prob_ >= 0.0f && independent_prob_ <= 0.5f,
                     "Failure probability must be between 0.0f and 0.5f");
  switch (mode_) {
  case tsuba::internal::FaultMode::RunLength: {
    fault_run_length_ = run_length;
    fmt::print("FaultTest RunLength {:d}\n", run_length_);
  } break;
  case tsuba::internal::FaultMode::UniformOverRun: {
    GALOIS_LOG_VASSERT(
        run_length > 0,
        "For UniformOverRun, max run length must be larger than 0");
    fault_run_length_ = galois::RandomUniformInt(1, run_length);
    fmt::print("FaultTest UniformOverRun {:d} ({:d})\n", fault_run_length_,
               run_length_);
  } break;
  case tsuba::internal::FaultMode::Independent: {
    fmt::print("FaultTest Independent {:f}\n", independent_prob_);
  } break;
  case tsuba::internal::FaultMode::None:
    // Do nothing
    break;
  }
}

static void die_now(const char* file, int line) {
  fmt::print("FaultTest::PtP {}:{}\n", file, line);
  // Best to kill ourselves quickly and messily
  *((volatile int*)0) = 1;
}

void tsuba::internal::PtP(const char* file, int line,
                          tsuba::internal::FaultSensitivity sensitivity) {
  ptp_count_++;
  switch (mode_) {
  case tsuba::internal::FaultMode::None:
    return;
  case tsuba::internal::FaultMode::Independent: {
    float threshold = independent_prob_;
    switch (sensitivity) {
    case tsuba::internal::FaultSensitivity::High: {
      threshold = 2 * independent_prob_;
      break;
    }
    case tsuba::internal::FaultSensitivity::Normal:
      // Do nothing
      break;
    }
    if (galois::RandomUniformFloat(1.0f) < threshold) {
      fmt::print("  PtP count {:d}\n", ptp_count_);
      die_now(file, line);
    }
  } break;
  case tsuba::internal::FaultMode::RunLength:
  case tsuba::internal::FaultMode::UniformOverRun: {
    if (ptp_count_ == fault_run_length_) {
      die_now(file, line);
    }
  }
  }
}

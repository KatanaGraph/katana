// FaultTest: support for injecting faults into Katana's storage layer in order
//  to test our crash recovery and transaction implementation

#include "katana/FaultTest.h"

#include "katana/Logging.h"
#include "katana/Random.h"

static katana::internal::FaultMode mode_{katana::internal::FaultMode::None};
static float independent_prob_{0.0f};
static uint64_t run_length_{UINT64_C(0)};
static uint64_t fault_run_length_{UINT64_C(0)};
static uint64_t ptp_count_{UINT64_C(0)};
static const std::unordered_map<katana::internal::FaultMode, std::string>
    fault_mode_label{
        {katana::internal::FaultMode::None, "No faults"},
        {katana::internal::FaultMode::Independent, "Independent"},
        {katana::internal::FaultMode::RunLength, "RunLength"},
        {katana::internal::FaultMode::UniformOverRun, "UniformOverRun"},
    };

void
katana::internal::FaultTestReport() {
  fmt::print("PtP count: {:d}\n", ptp_count_);
}

void
katana::internal::FaultTestInit(
    katana::internal::FaultMode mode, float independent_prob,
    uint64_t run_length) {
  mode_ = mode;
  independent_prob_ = independent_prob;
  run_length_ = run_length;
  // Configuration sanity check
  if (run_length > (1UL << 40)) {
    KATANA_LOG_WARN("Large run length {:d}", run_length);
  }
  KATANA_LOG_VASSERT(
      independent_prob_ >= 0.0f && independent_prob_ <= 0.5f,
      "Failure probability must be between 0.0f and 0.5f");
  switch (mode_) {
  case katana::internal::FaultMode::RunLength: {
    fault_run_length_ = run_length;
    fmt::print("FaultTest RunLength {:d}\n", run_length_);
  } break;
  case katana::internal::FaultMode::UniformOverRun: {
    KATANA_LOG_VASSERT(
        run_length > 2,
        "For UniformOverRun, max run length must be larger than 2");

    std::uniform_int_distribution<uint64_t> dist(2, run_length - 1);
    fault_run_length_ = dist(katana::GetGenerator());
    fmt::print(
        "FaultTest UniformOverRun {:d} ({:d})\n", fault_run_length_,
        run_length_);
  } break;
  case katana::internal::FaultMode::Independent: {
    fmt::print("FaultTest Independent {:f}\n", independent_prob_);
  } break;
  case katana::internal::FaultMode::None:
    // Do nothing
    break;
  }
}

static void
die_now(const char* file, int line) {
  fmt::print("FaultTest::PtP {}:{}\n", file, line);
  // Best to kill ourselves quickly and messily
  *((volatile int*)0) = 1;
}

void
katana::internal::PtP(
    const char* file, int line,
    katana::internal::FaultSensitivity sensitivity) {
  ptp_count_++;
  switch (mode_) {
  case katana::internal::FaultMode::None:
    return;
  case katana::internal::FaultMode::Independent: {
    float threshold = independent_prob_;
    switch (sensitivity) {
    case katana::internal::FaultSensitivity::High: {
      threshold = 2 * independent_prob_;
      break;
    }
    case katana::internal::FaultSensitivity::Normal:
      // Do nothing
      break;
    }
    std::uniform_real_distribution<float> dist(
        0, std::nextafter(1.0F, std::numeric_limits<float>::max()));
    if (dist(katana::GetGenerator()) < threshold) {
      fmt::print("  PtP count {:d}\n", ptp_count_);
      die_now(file, line);
    }
  } break;
  case katana::internal::FaultMode::RunLength:
  case katana::internal::FaultMode::UniformOverRun: {
    if (ptp_count_ == fault_run_length_) {
      die_now(file, line);
    }
  }
  }
}

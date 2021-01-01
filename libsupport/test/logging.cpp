#include "katana/Logging.h"

#include <system_error>

int
main() {
  KATANA_LOG_ERROR("string");
  KATANA_LOG_ERROR("format string: {}", 42);
  KATANA_LOG_ERROR("format string: {:d}", 42);
  // The following correctly fails with a compile time error
  // KATANA_LOG_ERROR("basic format string {:s}", 42);
  KATANA_LOG_WARN("format number: {:.2f}", 2.0 / 3.0);
  KATANA_LOG_WARN(
      "format error code: {}",
      std::make_error_code(std::errc::invalid_argument));
  KATANA_LOG_VERBOSE(
      "will be printed when environment variable KATANA_LOG_VERBOSE=1");
  KATANA_LOG_DEBUG("this will only be printed in debug builds");
  KATANA_LOG_ASSERT(1 == 1);

  return 0;
}

#include "galois/Logging.h"

#include <iostream>
#include <mutex>

#include "galois/GetEnv.h"

namespace {

void
PrintString(
    bool error, bool flush, const std::string& prefix, const std::string& s) {
  static std::mutex lock;
  std::lock_guard<std::mutex> lg(lock);

  std::ostream& o = error ? std::cerr : std::cout;
  if (!prefix.empty()) {
    o << prefix << ": ";
  }
  o << s << "\n";
  if (flush) {
    o.flush();
  }
}

}  // end unnamed namespace

void
galois::internal::LogString(galois::LogLevel level, const std::string& s) {
  int env_log_level = static_cast<int32_t>(LogLevel::Debug);
  GetEnv("GALOIS_LOG_LEVEL", &env_log_level);
  // Only log GALOIS_LOG_LEVEL and above (default, log everything)
  if (static_cast<int32_t>(level) < env_log_level) {
    return;
  }

  switch (level) {
  case LogLevel::Debug:
    return PrintString(true, false, "DEBUG", s);
  case LogLevel::Verbose:
    return PrintString(true, false, "VERBOSE", s);
  case LogLevel::Warning:
    return PrintString(true, false, "WARNING", s);
  case LogLevel::Error:
    return PrintString(true, false, "ERROR", s);
  default:
    std::abort();
  }
}

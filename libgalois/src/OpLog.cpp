#include "galois/OpLog.h"

#include "galois/Logging.h"

galois::Operation
galois::OpLog::GetOp(uint64_t index) const {
  if (index >= log_.size()) {
    GALOIS_LOG_WARN(
        "Log index {} >= {}, which is log size", index, log_.size());
  }
  return log_[index];
}

uint64_t
galois::OpLog::AppendOp(const Operation& op) {
  auto sz = log_.size();
  log_.emplace_back(op);
  return sz;
}

uint64_t
galois::OpLog::size() const {
  return log_.size();
}

void
galois::OpLog::Clear() {
  log_.clear();
}

galois::OpLog::OpLog(const galois::Uri& uri) {
  GALOIS_LOG_FATAL("Persistent log not yet implemented: {}", uri);
}

#include "katana/OpLog.h"

#include "katana/Logging.h"

katana::Operation
katana::OpLog::GetOp(uint64_t index) const {
  if (index >= log_.size()) {
    KATANA_LOG_WARN(
        "Log index {} >= {}, which is log size", index, log_.size());
  }
  return log_[index];
}

uint64_t
katana::OpLog::AppendOp(const Operation& op) {
  auto sz = log_.size();
  log_.emplace_back(op);
  return sz;
}

uint64_t
katana::OpLog::size() const {
  return log_.size();
}

void
katana::OpLog::Clear() {
  log_.clear();
}

katana::OpLog::OpLog(const katana::Uri& uri) {
  KATANA_LOG_FATAL("Persistent log not yet implemented: {}", uri);
}

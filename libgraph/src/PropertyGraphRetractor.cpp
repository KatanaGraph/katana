#include "katana/PropertyGraphRetractor.h"

template <typename T>
using Result = katana::Result<T>;

const katana::Uri&
katana::PropertyGraphRetractor::rdg_dir() const {
  return pg_->rdg_->rdg_dir();
}

void
katana::PropertyGraphRetractor::InformPath(const katana::Uri& storage_prefix) {
  pg_->rdg_->set_rdg_dir(storage_prefix);
}

Result<void>
katana::PropertyGraphRetractor::InformPath(const std::string& input_path) {
  if (!pg_->rdg_->rdg_dir().empty()) {
    KATANA_LOG_DEBUG("rdg dir from {} to {}", pg_->rdg_->rdg_dir(), input_path);
  }

  InformPath(KATANA_CHECKED(katana::Uri::Make(input_path)));
  return ResultSuccess();
}

Result<void>
katana::PropertyGraphRetractor::DropTopologies() {
  pg_->DropAllTopologies();
  return pg_->rdg_->DropAllTopologies();
}

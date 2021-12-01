#include "katana/PropertyGraphRetractor.h"

template <typename T>
using Result = katana::Result<T>;

Result<void>
katana::PropertyGraphRetractor::InformPath(const std::string& input_path) {
  if (!pg_->rdg_.rdg_dir().empty()) {
    KATANA_LOG_DEBUG("rdg dir from {} to {}", pg_->rdg_.rdg_dir(), input_path);
  }

  pg_->rdg_.set_rdg_dir(KATANA_CHECKED(katana::Uri::Make(input_path)));
  return ResultSuccess();
}

Result<void>
katana::PropertyGraphRetractor::DropTopologies() {
  //TODO: emcginnis reset all topologies in PGViewCache
  pg_->topology_ = std::make_shared<katana::GraphTopology>();
  return pg_->rdg_.DropAllTopologies();
}

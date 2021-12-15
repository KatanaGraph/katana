#include "katana/Experimental.h"

#include <iomanip>
#include <memory>

#include "katana/Env.h"
#include "katana/Logging.h"
#include "katana/Strings.h"

namespace {

class ExperimentalFeatureEnvState {
public:
  static ExperimentalFeatureEnvState* Get() {
    std::call_once(init_flag_, [&]() {
      state_ = std::unique_ptr<ExperimentalFeatureEnvState>(
          new ExperimentalFeatureEnvState());
    });
    return state_.get();
  }

  bool WasInEnv(const std::string& feature) {
    auto it = features_used_.find(feature);
    if (it == features_used_.end()) {
      return false;
    }
    it->second = true;
    return true;
  }

  const std::unordered_map<std::string, bool>& features_used() {
    return features_used_;
  }

private:
  ExperimentalFeatureEnvState() {
    std::string val;
    if (!katana::GetEnv("KATANA_ENABLE_EXPERIMENTAL", &val)) {
      return;
    }

    auto strings = katana::SplitView(val, ",");
    for (const auto& feature : strings) {
      features_used_.emplace(feature, false);
    }
  }

  std::unordered_map<std::string, bool> features_used_;

  static std::once_flag init_flag_;
  static std::unique_ptr<ExperimentalFeatureEnvState> state_;
};

std::once_flag ExperimentalFeatureEnvState::init_flag_;

std::unique_ptr<ExperimentalFeatureEnvState>
    ExperimentalFeatureEnvState::state_;

}  // namespace

katana::internal::ExperimentalFeature*
katana::internal::ExperimentalFeature::Register(
    const std::string& feature_name, const std::string& filename,
    int line_number) {
  std::lock_guard<std::mutex> lock(registered_features_mutex_);

  auto [it, was_created] = registered_features_.emplace(
      feature_name,
      std::unique_ptr<ExperimentalFeature>(
          new ExperimentalFeature(feature_name, filename, line_number)));
  const auto& flag = it->second;
  if (!was_created &&
      (flag->filename() != filename || flag->line_number() != line_number)) {
    KATANA_LOG_WARN(
        "{} declared in multiple places:\n\there: {}:{}\n\tand here: {}:{}",
        feature_name, flag->filename(), flag->line_number(), filename,
        line_number);
  }
  return it->second.get();
}

void
katana::internal::ExperimentalFeature::CheckEnv() {
  is_enabled_ = ExperimentalFeatureEnvState::Get()->WasInEnv(name_);
}

std::vector<std::string>
katana::internal::ExperimentalFeature::ReportEnabled() {
  std::lock_guard<std::mutex> lock(registered_features_mutex_);

  std::vector<std::string> enabled;

  for (const auto& [name, ptr] : registered_features_) {
    if (ptr->IsEnabled()) {
      enabled.emplace_back(name);
    }
  }
  return enabled;
}

std::vector<std::string>
katana::internal::ExperimentalFeature::ReportDisabled() {
  std::lock_guard<std::mutex> lock(registered_features_mutex_);

  std::vector<std::string> disabled;

  for (const auto& [name, ptr] : registered_features_) {
    if (!ptr->IsEnabled()) {
      disabled.emplace_back(name);
    }
  }
  return disabled;
}

std::vector<std::string>
katana::internal::ExperimentalFeature::ReportUnrecognized() {
  std::vector<std::string> unused;

  for (const auto& [name, was_used] :
       ExperimentalFeatureEnvState::Get()->features_used()) {
    if (!was_used) {
      unused.emplace_back(name);
    }
  }
  return unused;
}

std::mutex katana::internal::ExperimentalFeature::registered_features_mutex_;

std::unordered_map<
    std::string, std::unique_ptr<katana::internal::ExperimentalFeature>>
    katana::internal::ExperimentalFeature::registered_features_;

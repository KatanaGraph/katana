#ifndef KATANA_LIBSUPPORT_KATANA_EXPERIMENTAL_H_
#define KATANA_LIBSUPPORT_KATANA_EXPERIMENTAL_H_

#include <iomanip>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "katana/Logging.h"
#include "katana/config.h"

namespace katana::internal {

/// ExperimentalFeature tracks the state of feature flags set in the environment;
/// It is not intended to be used directly; please see the macro KATANA_EXPERIMENTAL_FEATURE
/// below
class KATANA_EXPORT ExperimentalFeature {
public:
  ExperimentalFeature(const char* name, const char* filename, int line_number)
      : name_(name), filename_(filename), line_number_(line_number) {
    std::lock_guard<std::mutex> lock(registered_features_mutex_);
    registered_features_.emplace(name_, this);
  }

  static bool IsEnabled(const char* feature_name) {
    auto it = registered_features_.find(feature_name);
    if (it == registered_features_.end()) {
      KATANA_LOG_DEBUG_VASSERT(
          false, "reference to unregistered feature flag {}", feature_name);
      return false;
    }
    it->second->CheckEnv();
    return it->second->is_enabled_;
  }

  /// report the feature flags that were enabled
  static std::vector<std::string> ReportEnabled();

  /// report the feature flags that were disabled
  static std::vector<std::string> ReportDisabled();

  /// report the feature flags that were provided but did not match any registered flag
  static std::vector<std::string> ReportUnrecognized();

  ExperimentalFeature(const ExperimentalFeature& no_copy) = delete;
  ExperimentalFeature& operator=(const ExperimentalFeature& no_copy) = delete;
  ExperimentalFeature(ExperimentalFeature&& no_move) = delete;
  ExperimentalFeature& operator=(ExperimentalFeature&& no_move) = delete;
  ~ExperimentalFeature() = default;

  const std::string& name() const { return name_; }
  const std::string& filename() const { return filename_; }
  int line_number() const { return line_number_; }

private:
  void CheckEnv();

  std::string name_;
  std::string filename_;
  int line_number_{};
  bool is_enabled_{};

  static std::mutex registered_features_mutex_;
  static std::unordered_map<std::string, ExperimentalFeature*>
      registered_features_;
};

}  // namespace katana::internal

/// KATANA_EXPERIMENTAL_FEATURE creates a flag that can be set from the environment.
/// The macro takes a feature_name which should be an unquoted, unique string that
/// looks like a function name. Developers can then use the macro
/// KATANA_EXPERIMENTAL_ENABLED using the same string to detect if the flag was set.
///
/// Flags are set using the environment variable KATANA_ENABLE_EXPERIMENTAL. Users
/// pass the same string passed to KATANA_EXPERIMENTAL_FEATURE to set a particular
/// flag. Multiple flags may be set by providing a comma delimited list of feature
/// names.
///
/// NB: an implication of the above is that these flags are only useful for runtime
/// configuration. If the desire is to control compile-time changes, a different
/// mechanism is required.
///
/// Example:
///
/// Program env:
///   KATANA_ENABLE_EXPERIMENTAL="UnstableButUseful"
///
/// active_development.cpp:
///    KATANA_EXPERIMENTAL_FEATURE(UnstableButUseful);
///
///    void important_function() {
///      if (KATANA_EXPERIMENTAL_ENABLED(UnstableButUseful)) {
///        // do something useful
///      } else {
///        // be conservative
///      }
///    }
///
/// Flags declared in different parts of the codebase can conflict. A good
/// practice is to choose good name for your feature and grep for this macro
/// to be sure it does not collide with another.
///
/// Another best practice is to heavily comment where the macro is defined,
/// detailing what the feature does and the state of tests to avoid regressions.
#define KATANA_EXPERIMENTAL_FEATURE(feature_name)                              \
  namespace katana::internal {                                                 \
  class KATANA_EXPORT ExperimentalFeature##feature_name                        \
      : public ExperimentalFeature {                                           \
  public:                                                                      \
    ExperimentalFeature##feature_name()                                        \
        : ExperimentalFeature(#feature_name, __FILE__, __LINE__) {}            \
  };                                                                           \
  const ExperimentalFeature##feature_name kFeatureFlag##feature_name;          \
  }                                                                            \
  static_assert(                                                               \
      std::is_same<                                                            \
          ::katana::internal::ExperimentalFeature,                             \
          katana::internal::ExperimentalFeature>::value,                       \
      "KATANA_EXPERIMENTAL_FEATURE must not be inside a namespace block")

#define KATANA_EXPERIMENTAL_ENABLED(feature_name)                              \
  (std::invoke([]() {                                                          \
    static std::once_flag oflag;                                               \
    static bool enabled;                                                       \
    std::call_once(oflag, [&]() {                                              \
      enabled =                                                                \
          ::katana::internal::ExperimentalFeature::IsEnabled(#feature_name);   \
    });                                                                        \
    return enabled;                                                            \
  }))

#endif

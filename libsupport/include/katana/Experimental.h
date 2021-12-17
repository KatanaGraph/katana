#ifndef KATANA_LIBSUPPORT_KATANA_EXPERIMENTAL_H_
#define KATANA_LIBSUPPORT_KATANA_EXPERIMENTAL_H_

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "katana/config.h"

namespace katana {
namespace internal {

/// ExperimentalFeature tracks the state of feature flags set in the environment;
/// It is not intended to be used directly; please see the macro KATANA_EXPERIMENTAL_FEATURE
/// below
class KATANA_EXPORT ExperimentalFeature {
public:
  static ExperimentalFeature* Register(
      const std::string& feature_name, const std::string& filename,
      int line_number);

  /// report the feature flags that were checked on codepaths that were executed and
  /// the flag was set to true
  static std::vector<std::string> ReportEnabled();

  /// report the feature flags that were used but stayed false
  static std::vector<std::string> ReportDisabled();

  /// report the feature flags that were provided but did not match any registered flag
  static std::vector<std::string> ReportUnrecognized();

  bool IsEnabled() { return is_enabled_; }

  ExperimentalFeature(const ExperimentalFeature& no_copy) = delete;
  ExperimentalFeature& operator=(const ExperimentalFeature& no_copy) = delete;
  ExperimentalFeature(ExperimentalFeature&& no_move) = delete;
  ExperimentalFeature& operator=(ExperimentalFeature&& no_move) = delete;
  ~ExperimentalFeature() = default;

  const std::string& name() const { return name_; }
  const std::string& filename() const { return filename_; }
  int line_number() const { return line_number_; }

private:
  ExperimentalFeature(std::string name, std::string filename, int line_number)
      : name_(std::move(name)),
        filename_(std::move(filename)),
        line_number_(line_number) {
    CheckEnv();
  }

  void CheckEnv();

  std::string name_;
  std::string filename_;
  int line_number_{};
  bool is_enabled_{};

  static std::mutex registered_features_mutex_;
  static std::unordered_map<std::string, std::unique_ptr<ExperimentalFeature>>
      registered_features_;
};

}  // namespace internal
}  // namespace katana

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
  class ExperimentalFeature;                                                   \
  static auto* katana_experimental_feature_ptr_##feature_name =                \
      ::katana::internal::ExperimentalFeature::Register(                       \
          #feature_name, __FILE__, __LINE__);                                  \
  }                                                                            \
  static_assert(                                                               \
      std::is_same<                                                            \
          ::katana::internal::ExperimentalFeature,                             \
          katana::internal::ExperimentalFeature>::value,                       \
      "KATANA_EXPERIMENTAL_FEATURE must not be inside a namespace block")

#define KATANA_EXPERIMENTAL_ENABLED(feature_name)                              \
  (::katana::internal::katana_experimental_feature_ptr_##feature_name          \
       ->IsEnabled())

#endif

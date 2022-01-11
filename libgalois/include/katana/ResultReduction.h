#pragma once

#include <optional>

#include "katana/Reduction.h"
#include "katana/Result.h"

namespace katana {

namespace internal {

struct AnyCopyableErrorInfo {
  std::optional<CopyableErrorInfo> operator()(
      const std::optional<CopyableErrorInfo>& lhs,
      const std::optional<CopyableErrorInfo>& rhs) const {
    if (lhs) {
      return *lhs;
    }
    if (rhs) {
      return *rhs;
    }
    return std::nullopt;
  }
};

struct IdentityCopyableErrorInfo {
  std::optional<CopyableErrorInfo> operator()() const { return std::nullopt; }
};

}  // namespace internal

/// This class is thread-safe and supports concurrently adding error infos.
/// However, it currently supports only returning one of the error infos.
/// TODO(roshan/amp) combine/merge all distinct error infos into one error info
class CombinedErrorInfo
    : public Reducible<
          std::optional<CopyableErrorInfo>, internal::AnyCopyableErrorInfo,
          internal::IdentityCopyableErrorInfo> {
  using base_type = Reducible<
      std::optional<CopyableErrorInfo>, internal::AnyCopyableErrorInfo,
      internal::IdentityCopyableErrorInfo>;

public:
  CombinedErrorInfo()
      : base_type(
            internal::AnyCopyableErrorInfo(),
            internal::IdentityCopyableErrorInfo()) {}

  std::optional<CopyableErrorInfo> operator()() { return base_type::reduce(); }
};

#define KATANA_COMBINE_ERROR_IMPL(                                             \
    combined_error, result_name, expression, ...)                              \
  ({                                                                           \
    auto result_name = (expression);                                           \
    if (::katana::internal::CheckedExpressionFailed(result_name)) {            \
      combined_error.update(                                                   \
          ::katana::internal::CheckedExpressionToError(result_name)            \
              .WithContext(__VA_ARGS__)                                        \
              .WithContext(FMT_STRING("({}:{})"), __FILE__, __LINE__));        \
      return;                                                                  \
    }                                                                          \
    std::move(                                                                 \
        ::katana::internal::CheckedExpressionToValue(std::move(result_name))); \
  })

/// KATANA_COMBINE_ERROR_CONTEXT is similar to KATANA_CHECKED_CONTEXT
/// except that instead of returning the error, it adds the error
/// into \p combined_error - an object of CombinedErrorInfo - by calling
/// CombinedErrorInfo::update(CopyableErrorInfo)
#define KATANA_COMBINE_ERROR_CONTEXT(                                          \
    combined_error, expression, format_str, ...)                               \
  KATANA_COMBINE_ERROR_IMPL(                                                   \
      combined_error, KATANA_CHECKED_NAME(_error_or_value, __COUNTER__),       \
      expression, FMT_STRING(format_str), ##__VA_ARGS__)

/// KATANA_COMBINE_ERROR_CODE is similar to KATANA_CHECKED_ERROR_CODE
/// except that instead of returning the error, it adds the error
/// into \p combined_error - an object of CombinedErrorInfo - by calling
/// CombinedErrorInfo::update(CopyableErrorInfo)
#define KATANA_COMBINE_ERROR_CODE(                                             \
    combined_error, expression, error_code, format_str, ...)                   \
  KATANA_COMBINE_ERROR_IMPL(                                                   \
      combined_error, KATANA_CHECKED_NAME(_error_or_value, __COUNTER__),       \
      expression, error_code, FMT_STRING(format_str), ##__VA_ARGS__)

/// KATANA_COMBINE_ERROR is similar to KATANA_CHECKED
/// except that instead of returning the error, it adds the error
/// into \p combined_error - an object of CombinedErrorInfo - by calling
/// CombinedErrorInfo::update(CopyableErrorInfo)
#define KATANA_COMBINE_ERROR(combined_error, expression)                       \
  KATANA_COMBINE_ERROR_CONTEXT(combined_error, expression, "backtrace")

}  // namespace katana

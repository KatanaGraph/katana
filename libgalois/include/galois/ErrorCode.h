#ifndef GALOIS_LIBGALOIS_GALOIS_ERROR_CODE_H_
#define GALOIS_LIBGALOIS_GALOIS_ERROR_CODE_H_

#include <string>
#include <system_error>

namespace galois {

enum class ErrorCode {
  Success         = 0,
  InvalidArgument = 1,
  ArrowError      = 2,
  NotImplemented  = 3,
  NotFound        = 4,
};

} // namespace galois

namespace galois::internal {

class ErrorCodeCategory : public std::error_category {
public:
  const char* name() const noexcept final { return "GaloisError"; }

  std::string message(int c) const final {
    switch (static_cast<ErrorCode>(c)) {
    case ErrorCode::Success:
      return "success";
    case ErrorCode::InvalidArgument:
      return "invalid argument";
    case ErrorCode::ArrowError:
      return "arrow error";
    case ErrorCode::NotImplemented:
      return "not implemented";
    case ErrorCode::NotFound:
      return "not found";
    default:
      return "unknown error";
    }
  }

  std::error_condition default_error_condition(int c) const noexcept final {
    switch (static_cast<ErrorCode>(c)) {
    case ErrorCode::InvalidArgument:
    case ErrorCode::ArrowError:
      return make_error_condition(std::errc::invalid_argument);
    case ErrorCode::NotImplemented:
      return make_error_condition(std::errc::function_not_supported);
    case ErrorCode::NotFound:
      return make_error_condition(std::errc::no_such_file_or_directory);
    default:
      return std::error_condition(c, *this);
    }
  }
};

/// Return singleton category
const ErrorCodeCategory& GetErrorCodeCategory();

} // namespace galois::internal

namespace std {

/// Tell STL about our error code.
template <>
struct is_error_code_enum<galois::ErrorCode> : true_type {};

} // namespace std

namespace galois {

/// Overload free function make_error_code with our error code. This will be
/// found with ADL if necessary.
inline std::error_code make_error_code(ErrorCode e) noexcept {
  return {static_cast<int>(e), internal::GetErrorCodeCategory()};
}

} // namespace galois

#endif

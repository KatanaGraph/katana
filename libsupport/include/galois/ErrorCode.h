#ifndef GALOIS_LIBSUPPORT_GALOIS_ERRORCODE_H_
#define GALOIS_LIBSUPPORT_GALOIS_ERRORCODE_H_

#include <string>
#include <system_error>

#include "galois/config.h"

namespace galois {

enum class ErrorCode {
  Success = 0,
  InvalidArgument = 1,
  NotImplemented = 2,
  NotFound = 3,
  ArrowError = 4,
  JsonParseFailed = 5,
  JsonDumpFailed = 6,
  HttpError = 7,
};

}  // namespace galois

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
    case ErrorCode::NotImplemented:
      return "not implemented";
    case ErrorCode::NotFound:
      return "not found";
    case ErrorCode::ArrowError:
      return "arrow error";
    case ErrorCode::JsonParseFailed:
      return "could not parse json";
    case ErrorCode::JsonDumpFailed:
      return "could not dump json";
    case ErrorCode::HttpError:
      return "http operation failed";
    default:
      return "unknown error";
    }
  }

  std::error_condition default_error_condition(int c) const noexcept final {
    switch (static_cast<ErrorCode>(c)) {
    case ErrorCode::InvalidArgument:
    case ErrorCode::ArrowError:
    case ErrorCode::JsonParseFailed:
    case ErrorCode::JsonDumpFailed:
      return make_error_condition(std::errc::invalid_argument);
    case ErrorCode::NotImplemented:
      return make_error_condition(std::errc::function_not_supported);
    case ErrorCode::NotFound:
      return make_error_condition(std::errc::no_such_file_or_directory);
    case ErrorCode::HttpError:
      return make_error_condition(std::errc::io_error);
    default:
      return std::error_condition(c, *this);
    }
  }
};

/// Return singleton category
GALOIS_EXPORT const ErrorCodeCategory& GetErrorCodeCategory();

}  // namespace galois::internal

namespace std {

/// Tell STL about our error code.
template <>
struct is_error_code_enum<galois::ErrorCode> : true_type {};

}  // namespace std

namespace galois {

/// Overload free function make_error_code with our error code. This will be
/// found with ADL if necessary.
inline std::error_code
make_error_code(ErrorCode e) noexcept {
  return {static_cast<int>(e), internal::GetErrorCodeCategory()};
}

}  // namespace galois

#endif

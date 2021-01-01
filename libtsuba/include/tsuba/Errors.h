#ifndef KATANA_LIBTSUBA_TSUBA_ERRORS_H_
#define KATANA_LIBTSUBA_TSUBA_ERRORS_H_

#include <system_error>

#include <arrow/api.h>

#include "katana/config.h"

namespace tsuba {

enum class ErrorCode {
  // Success = 0,
  InvalidArgument = 1,
  ArrowError = 2,
  NotImplemented = 3,
  NotFound = 4,
  Exists = 5,
  OutOfMemory = 6,
  TODO = 7,
  S3Error = 8,
  AWSWrongRegion = 9,
  PropertyNotFound = 10,
  LocalStorageError = 12,
  NoCredentials = 13,
  AzureError = 14,
  MpiError = 15,
  BadVersion = 16,
  GSError = 17,
};

KATANA_EXPORT ErrorCode ArrowToTsuba(arrow::StatusCode);

}  // namespace tsuba

namespace tsuba::internal {

class KATANA_EXPORT ErrorCodeCategory : public std::error_category {
public:
  const char* name() const noexcept final { return "TsubaError"; }

  std::string message(int c) const final {
    switch (static_cast<ErrorCode>(c)) {
    case ErrorCode::InvalidArgument:
      return "invalid argument";
    case ErrorCode::ArrowError:
      return "arrow error";
    case ErrorCode::NotImplemented:
      return "not implemented";
    case ErrorCode::NotFound:
      return "not found";
    case ErrorCode::Exists:
      return "already exists";
    case ErrorCode::TODO:
      return "TODO error yet to be classified";
    case ErrorCode::S3Error:
      return "S3 error";
    case ErrorCode::AWSWrongRegion:
      return "AWS op may succeed in other region";
    case ErrorCode::PropertyNotFound:
      return "no such property";
    case ErrorCode::LocalStorageError:
      return "local storage error";
    case ErrorCode::NoCredentials:
      return "credentials not configured";
    case ErrorCode::AzureError:
      return "Azure error";
    case ErrorCode::BadVersion:
      return "previous version expectation violated";
    case ErrorCode::MpiError:
      return "some MPI process reported an error";
    case ErrorCode::GSError:
      return "Google storage error";
    default:
      return "unknown error";
    }
  }

  std::error_condition default_error_condition(int c) const noexcept final {
    switch (static_cast<ErrorCode>(c)) {
    case ErrorCode::InvalidArgument:
    case ErrorCode::ArrowError:
    case ErrorCode::PropertyNotFound:
    case ErrorCode::NoCredentials:
    case ErrorCode::BadVersion:
      return make_error_condition(std::errc::invalid_argument);
    case ErrorCode::NotImplemented:
      return make_error_condition(std::errc::function_not_supported);
    case ErrorCode::NotFound:
      return make_error_condition(std::errc::no_such_file_or_directory);
    case ErrorCode::Exists:
      return make_error_condition(std::errc::file_exists);
    case ErrorCode::AWSWrongRegion:
    case ErrorCode::S3Error:
    case ErrorCode::LocalStorageError:
    case ErrorCode::AzureError:
    case ErrorCode::MpiError:
    case ErrorCode::GSError:
      return make_error_condition(std::errc::io_error);
    default:
      return std::error_condition(c, *this);
    }
  }
};

/// Return singleton category
KATANA_EXPORT const ErrorCodeCategory& GetErrorCodeCategory();

}  // namespace tsuba::internal

namespace std {

/// Tell STL about our error code.
template <>
struct is_error_code_enum<tsuba::ErrorCode> : true_type {};

}  // namespace std

namespace tsuba {

/// Overload free function make_error_code with our error code. This will be
/// found with ADL if necessary.
inline std::error_code
make_error_code(ErrorCode e) noexcept {
  return {static_cast<int>(e), internal::GetErrorCodeCategory()};
}

}  // namespace tsuba

#endif

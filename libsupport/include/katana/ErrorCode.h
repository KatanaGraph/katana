#ifndef KATANA_LIBSUPPORT_KATANA_ERRORCODE_H_
#define KATANA_LIBSUPPORT_KATANA_ERRORCODE_H_

#include <string>
#include <system_error>

#include "katana/config.h"

/// The STL provides a general mechanism for defining error codes that is
/// intended to be portable across libraries:
///
/// - An std::error_code is an integer (called an error enum) plus a pointer to
///   an std::error_category.
/// - Various methods on error codes like getting the error message are
///   implemented by calling a method on the std::error_category with the
///   integer from the std::error_code
///
/// This particular representation of an integer plus a pointer allows an
/// std::error_code to behave like a traditional error code (i.e., like an
/// integer), maintains a compact and uniform representation and provides for
/// namespaced error codes.
///
/// An std::error_code is intended to model a specific, possibly
/// non-portable, error. An std::error_condition is intended to model a
/// general class of errors that callers can portably compare against. E.g.,
///
///   if (error_code == error_condition) { .... }
///
/// The representation of both std::error_code and std::error_condition is the
/// same: an integer plus a pointer to an std::error_category. You can think of
/// std::error_code and std::error_condition as a two-level hierarchy where
/// multiple std::error_codes are grouped into an std::error_condition.
/// std::error_codes are mapped to an std::error_condition via
/// std::error_category::default_error_condition and
/// std::error_category::equivalent.
///
/// The way to create a new std::error_code is to:
/// - Define an error enum
/// - Define a new error category that is a subclass of std::error_category
/// - Tell the STL about your enum by specializing std::is_error_code_enum
///   to your enum if an enum class.
/// - Tell the STL how to create a std::error_code from your enum by defining
///   make_error_code for your enum. This function will be found by argument
///   dependent lookup so must be in the same namespace as the error enum.
///
/// \file ErrorCode.h

namespace katana {

enum class ErrorCode {
  // It is probably a bug to return Success explicitly rather than using
  // something like ResultSuccess(). Comment it out to be safe.
  //
  // Success = 0,
  InvalidArgument = 1,
  NotImplemented = 2,
  NotFound = 3,
  ArrowError = 4,
  JsonParseFailed = 5,
  JsonDumpFailed = 6,
  HttpError = 7,
  TODO = 8,
  PropertyNotFound = 9,
  AlreadyExists = 10,
  TypeError = 11,
  AssertionFailed = 12,
  GraphUpdateFailed = 13,
};

}  // namespace katana

namespace katana::internal {

class KATANA_EXPORT ErrorCodeCategory : public std::error_category {
public:
  ~ErrorCodeCategory() override;

  const char* name() const noexcept final { return "GaloisError"; }

  std::string message(int c) const final {
    switch (static_cast<ErrorCode>(c)) {
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
    case ErrorCode::TODO:
      return "TODO";
    case ErrorCode::PropertyNotFound:
      return "no such property";
    case ErrorCode::AlreadyExists:
      return "already exists";
    case ErrorCode::TypeError:
      return "type error";
    case ErrorCode::AssertionFailed:
      return "assertion failed";
    case ErrorCode::GraphUpdateFailed:
      return "graph update failed";
    default:
      return "unknown error";
    }
  }

  std::error_condition default_error_condition(int c) const noexcept final {
    switch (static_cast<ErrorCode>(c)) {
    case ErrorCode::TODO:
    case ErrorCode::InvalidArgument:
    case ErrorCode::ArrowError:
    case ErrorCode::JsonParseFailed:
    case ErrorCode::JsonDumpFailed:
    case ErrorCode::TypeError:
    case ErrorCode::AssertionFailed:
    case ErrorCode::GraphUpdateFailed:
      return make_error_condition(std::errc::invalid_argument);
    case ErrorCode::AlreadyExists:
      return make_error_condition(std::errc::file_exists);
    case ErrorCode::NotImplemented:
      return make_error_condition(std::errc::function_not_supported);
    case ErrorCode::NotFound:
    case ErrorCode::PropertyNotFound:
      return make_error_condition(std::errc::no_such_file_or_directory);
    case ErrorCode::HttpError:
      return make_error_condition(std::errc::io_error);
    default:
      return std::error_condition(c, *this);
    }
  }
};

/// Return singleton category
KATANA_EXPORT const ErrorCodeCategory& GetErrorCodeCategory();

}  // namespace katana::internal

namespace std {

/// Tell STL about our error code enum.
template <>
struct is_error_code_enum<katana::ErrorCode> : true_type {};

}  // namespace std

namespace katana {

/// make_error_code converts ErrorCode into a standard error code. It is an STL
/// and outcome extension point and will be found with ADL if necessary.
inline std::error_code
make_error_code(ErrorCode e) noexcept {
  return {static_cast<int>(e), internal::GetErrorCodeCategory()};
}

}  // namespace katana

#endif

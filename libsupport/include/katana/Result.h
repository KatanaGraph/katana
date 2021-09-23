#ifndef KATANA_LIBSUPPORT_KATANA_RESULT_H_
#define KATANA_LIBSUPPORT_KATANA_RESULT_H_

#include <cerrno>
#include <cstring>
#include <iterator>
#include <ostream>
#include <system_error>

#include <arrow/result.h>
#include <boost/outcome/outcome.hpp>
#include <boost/outcome/trait.hpp>
#include <boost/outcome/utils.hpp>
#include <fmt/format.h>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/config.h"

/// Code that needs to indicate an error to callers typically should use
/// `katana::Result`. When a function returns `katana::Result<T>`, it means that
/// the function either returns a `T` or an error. When a function returns
/// `katana::Result<void>`, it means that either the function succeeds (i.e.,
/// returns `void`) or returns an error.
///
/// A common pattern for using a function that returns a `katana::Result<T>` is:
///
///     auto r = ReturnsAResultWithValue();
///     if (!r) {
///       // Some error happened. Either...
///
///       // ... we handle it
///       if (r.error() == ErrorCode::Foo) {
///         return DoAlternative();
///       }
///
///       // ... or we propagate it
///       return r.error();
///     }
///
///     // No error happened. Continue on.
///     T value = std::move(r.value());
///
///
/// If you are looking to simplify error handling, there is a macro
/// `KATANA_CHECKED` which simplifies the pattern of:
///
///     auto r1 = ReturnsAResultWithValue();
///     if (!r1) {
///       return r1.error();
///     }
///     T value = std::move(r1.value());
///
///     auto r2 = ReturnsAResult();
///     if (!r2) {
///       return r2.error();
///     }
///
/// to
///
///     T value = KATANA_CHECKED(ReturnsAResultWithValue());
///
///     KATANA_CHECKED(ReturnsAResult());
///
/// Code should be exception-safe, but exceptions are rarely thrown in the
/// codebase. Exceptions are reserved for situations where it is equally acceptable
/// to terminate the current process, which is rare in library code, or as a safe
/// implementation of `setjmp`/`longjmp`, which is even rarer.
///
/// Errors are a part of the contract between a caller and callee in the same way
/// parameters and return values are. When writing an error message or selecting an
/// error code, consider the perspective of the caller and their natural first
/// question: "how can I make this error go away?"
///
/// Good messages should be to the point and in terms of the caller and not
/// artifacts of an implementation detail in the callee.
///
/// Compare
///
///     Result<void> CheckNumber(int number) {
///       if (number == 0) {
///         return KATANA_ERROR(IllegalArgument, "cannot divide by zero");
///       }
///       int u = n / number;
///     }
///
/// and
///
///     Result<void> CheckNumber(int number) {
///       if (number == 0) {
///         return KATANA_ERROR(IllegalArgument, "number should be positive");
///       }
///       int u = n / number;
///     }
///
/// We consider the second code snippet better because it provides a hint on what
/// the user can do, rather than just mentioning the error happening in the
/// library.
///
/// As a matter of consistency and style, messages should begin with a lowercase
/// letter to avoid switching between different case styles when errors are
/// propagated.
///
/// For example,
///
///     Result<void> MakeList() {
///       ...
///       if (auto r = CheckNumber(n); !r) {
///         return r.error().WithContext("making number {}", n)
///       }
///     }
///
///     // or more concisely...
///
///     Result<void> MakeList() {
///       ...
///       KATANA_CHECKED_CONTEXT(CheckNumber(n), "making number {}", n);
///     }
///
/// will create error strings like `making number 0: number should be positive`.
///
/// On older compilers, auto conversion to `katana::Result` will fail for types
/// that can't be copied. One symptom is compiler errors on GCC 7 but not on GCC 9.
/// We've adopted the workaround of returning such objects like so:
///
///     Result<Thing> MakeMoveOnlyThing() {
///       Thing t;
///       ....
///       return MakeResult(std::move(t));
///     }
///
/// \file

namespace katana {

namespace internal {

struct abort_policy : BOOST_OUTCOME_V2_NAMESPACE::policy::base {
  template <class Impl>
  static constexpr void wide_value_check(Impl&& self) {
    if (!base::_has_value(std::forward<Impl>(self))) {
      AbortApplication();
    }
  }

  template <class Impl>
  static constexpr void wide_error_check(Impl&& self) {
    if (!base::_has_error(std::forward<Impl>(self))) {
      AbortApplication();
    }
  }

  template <class Impl>
  static constexpr void wide_exception_check(Impl&& self) {
    if (!base::_has_exception(std::forward<Impl>(self))) {
      AbortApplication();
    }
  }
};

}  // namespace internal

class CopyableErrorInfo;

/// An ErrorInfo contains additional context about an error in addition to an
/// error code. It works together with Result and user-defined error codes.
///
/// An example of use:
///
///     Result<One> MakeOne() {
///       if (...) {
///         // Return an error without any additional context
///         return ErrorCode::BadFoo;
///       } else if (...) {
///         // Return an error with context
///         return KATANA_ERROR(ErrorCode::BadFoo, "context message {}", ...);
///       }
///
///       return One();
///     }
///
///     Result<Many> MakeMany() {
///       for (...) {
///         auto r = MakeOne();
///         if (!r) {
///           if (r == ErrorCode::NoFoo) {
///             // Handle an error
///             continue;
///           }
///
///           // Propagate an error
///           return r.error().WithContext("making many");
///         }
///       }
///
///       return Many();
///     }
///
///     void User() {
///       auto r = MakeMany();
///       if (!r) {
///         std::cout << "error " << r.error() << "\n";
///         // or...
///         KATANA_LOG_ERROR("error {}", r.error());
///       }
///     }
///
/// Returning an error code or using KATANA_ERROR are two ways to create an
/// initial error, and ErrorInfo::WithContext is used to add context to an
/// error as it is returned.
///
/// An ErrorInfo is intended to propagate errors up a call stack for a single
/// thread. If you want to store an ErrorInfo for a brief period of time, you
/// should save an ErrorInfo with a CopyableErrorInfo.
///
/// Because an ErrorInfo models an error code, which in turn models good old
/// errno (int), an ErrorInfo is equivalent to another ErrorInfo based solely
/// on their error codes and is not dependent on any additional error context
/// information.
///
/// See ErrorCode.h for an example of defining a new error code and how error
/// codes are compared.
class KATANA_EXPORT [[nodiscard]] ErrorInfo {
public:
  class Context;

  static constexpr int kContextSize = 512;

  ErrorInfo() : ErrorInfo(std::error_code()) {}

  ErrorInfo(const std::error_code& ec) : error_code_(ec) {}

  template <
      typename ErrorEnum, typename U = std::enable_if_t<
                              std::is_error_code_enum_v<ErrorEnum> ||
                              std::is_error_condition_enum_v<ErrorEnum>>>
  ErrorInfo(ErrorEnum && err)
      : ErrorInfo(make_error_code(std::forward<ErrorEnum>(err))) {}

  /// Construct an ErrorInfo with a context message that overrides
  /// ec.message()
  ErrorInfo(const std::error_code& ec, const std::string& context)
      : ErrorInfo(ec) {
    Prepend(context.c_str(), context.c_str() + context.size());
  }

  ErrorInfo(const CopyableErrorInfo& cei);

  const std::error_code& error_code() const { return error_code_; }

  /// MakeWithSourceInfo makes an ErrorInfo from a root error with additional
  /// arguments passed to fmt::format
  template <typename F, typename... Args>
  static ErrorInfo MakeWithSourceInfo(
      const char* file_name, int line_no, const std::error_code& ec,
      F fmt_string, Args&&... args) {
    fmt::memory_buffer out;
    fmt::format_to(
        std::back_inserter(out), fmt_string, std::forward<Args>(args)...);
    const char* base_name = std::strrchr(file_name, '/');
    if (!base_name) {
      base_name = file_name;
    } else {
      base_name++;
    }

    fmt::format_to(std::back_inserter(out), " ({}:{})", base_name, line_no);

    ErrorInfo ei(ec);
    ei.Prepend(out.data(), out.data() + out.size());

    return ei;
  }

  template <typename F, typename... Args>
  ErrorInfo WithContext(F && fmt_string, Args && ... args) {
    SpillMessage();

    PrependFmt(std::forward<F>(fmt_string), std::forward<Args>(args)...);

    return *this;
  }

  template <typename ErrorEnum, typename F, typename... Args>
  std::enable_if_t<
      std::is_error_code_enum_v<ErrorEnum> ||
          std::is_error_condition_enum_v<ErrorEnum>,
      ErrorInfo>
  WithContext(ErrorEnum err, F && fmt_string, Args && ... args) {
    SpillMessage();

    error_code_ = make_error_code(err);

    PrependFmt(std::forward<F>(fmt_string), std::forward<Args>(args)...);

    return *this;
  }

  std::ostream& Write(std::ostream & out) const;

private:
  template <typename F, typename... Args>
  void PrependFmt(F fmt_string, Args && ... args) {
    std::vector<char> out;
    fmt::format_to(
        std::back_inserter(out), fmt_string, std::forward<Args>(args)...);
    Prepend(out.data(), out.data() + out.size());
  }

  void Prepend(const char* begin, const char* end);

  /// SpillMessage writes the current error_code message to the error
  /// context if the error context is empty
  void SpillMessage();

  void CheckContext();

  std::error_code error_code_;
  std::pair<Context*, int> context_{};
};

inline std::ostream&
operator<<(std::ostream& out, const ErrorInfo& ei) {
  return ei.Write(out);
}

/// KATANA_ERROR creates new ErrorInfo and records information about the
/// callsite (e.g., line number).
#define KATANA_ERROR(ec, fmt_string, ...)                                      \
  ::katana::ErrorInfo::MakeWithSourceInfo(                                     \
      __FILE__, __LINE__, (ec), FMT_STRING(fmt_string), ##__VA_ARGS__)

/// A CopyableErrorInfo is like an ErrorInfo but used outside a thread's error
/// stack.
///
/// An ErrorInfo is not generally copyable because it references thread
/// local data. A CopyableErrorInfo is useful in cases where one wants
/// to store errors, e.g., collecting results across threads.
class KATANA_EXPORT CopyableErrorInfo {
public:
  CopyableErrorInfo(const std::error_code& ec) : error_code_(ec) {}

  CopyableErrorInfo() : CopyableErrorInfo(std::error_code()) {}

  CopyableErrorInfo(const ErrorInfo& ei);

  template <
      typename ErrorEnum, typename U = std::enable_if_t<
                              std::is_error_code_enum_v<ErrorEnum> ||
                              std::is_error_condition_enum_v<ErrorEnum>>>
  CopyableErrorInfo(ErrorEnum&& err)
      : CopyableErrorInfo(make_error_code(std::forward<ErrorEnum>(err))) {}

  template <typename F, typename... Args>
  CopyableErrorInfo WithContext(F&& fmt_string, Args&&... args) {
    PrependFmt(std::forward<F>(fmt_string), std::forward<Args>(args)...);

    return *this;
  }

  template <typename ErrorEnum, typename F, typename... Args>
  std::enable_if_t<
      std::is_error_code_enum_v<ErrorEnum> ||
          std::is_error_condition_enum_v<ErrorEnum>,
      CopyableErrorInfo>
  WithContext(ErrorEnum err, F&& fmt_string, Args&&... args) {
    error_code_ = make_error_code(err);

    PrependFmt(std::forward<F>(fmt_string), std::forward<Args>(args)...);

    return *this;
  }

  const std::error_code& error_code() const { return error_code_; }

  const std::string& message() const { return message_; }

  std::ostream& Write(std::ostream& out) const;

private:
  template <typename F, typename... Args>
  void PrependFmt(F fmt_string, Args&&... args) {
    std::vector<char> out;
    fmt::format_to(
        std::back_inserter(out), fmt_string, std::forward<Args>(args)...);
    Prepend(out.data(), out.data() + out.size());
  }

  void Prepend(const char* begin, const char* end) {
    if (!message_.empty()) {
      message_.insert(0, std::string(": "));
    }
    message_.insert(message_.begin(), begin, end);
  }

  std::error_code error_code_;
  std::string message_;
};

inline std::ostream&
operator<<(std::ostream& out, const CopyableErrorInfo& ei) {
  return ei.Write(out);
}

}  // namespace katana

// Tell boost::outcome which types will be used as an error type E in
// std_result<T, E, ...> below.
// We inject the trait as per https://www.boost.org/doc/libs/1_70_0/libs/outcome/doc/html/reference/traits/is_error_type.html
// "Overridable: By template specialisation into the trait namespace."
// It is safer to define the trait before the definition of the Result structure.
BOOST_OUTCOME_V2_NAMESPACE_BEGIN

namespace trait {

template <>
struct is_error_type<katana::ErrorInfo> {
  static constexpr bool value = true;
};

template <>
struct is_error_type<katana::CopyableErrorInfo> {
  static constexpr bool value = true;
};

}  // namespace trait

BOOST_OUTCOME_V2_NAMESPACE_END

namespace katana {

/// make_error_code converts ErrorInfo into a standard error code. It is an STL
/// and boost::outcome extension point and will be found with ADL if necessary.
inline std::error_code
make_error_code(ErrorInfo e) noexcept {
  return e.error_code();
}

/// make_error_code converts CopyableErrorInfo into a standard error code. It is
/// an STL and boost::outcome extension point and will be found with ADL if
/// necessary.
inline std::error_code
make_error_code(CopyableErrorInfo e) noexcept {
  return e.error_code();
}

/// A Result is a T or an ErrorInfo.
template <class T>
using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<
    T, ErrorInfo, internal::abort_policy>;

/// MakeResult is a workaround for older compilers that do not naturally
/// convert move-only values into Results in return statements.
///
/// For example, in recent compilers, the following compiles:
///
///     Result<Thing> MakeMoveOnlyThing() {
///       Thing t;
///       ....
///       return t;
///     }
///
/// But on older compilers, you might see an error about copying a value that
/// cannot be copied.
///
/// In these cases, you can use this pattern instead:
///
///     Result<Thing> MakeMoveOnlyThing() {
///       Thing t;
///       ....
///       return MakeResult(std::move(t));
///     }
template <typename T>
Result<T>
MakeResult(T&& val) noexcept {
  // TODO(amber): Remove this when we have more recent compilers that can
  // construct Result<T> from T in a function's return statement.
  return Result<T>(std::forward<T>(val));
}

/// A CopyableResult is a T or an CopyableErrorInfo.
template <class T>
using CopyableResult = BOOST_OUTCOME_V2_NAMESPACE::std_result<
    T, CopyableErrorInfo, internal::abort_policy>;

inline bool
operator==(const ErrorInfo& a, const ErrorInfo& b) {
  return make_error_code(a) == make_error_code(b);
}

inline bool
operator!=(const ErrorInfo& a, const ErrorInfo& b) {
  return !(a == b);
}

inline bool
operator==(const CopyableErrorInfo& a, const CopyableErrorInfo& b) {
  return make_error_code(a) == make_error_code(b);
}

inline bool
operator!=(const CopyableErrorInfo& a, const CopyableErrorInfo& b) {
  return !(a == b);
}

// TODO (serge): make these functions back inline after the issue in nvcc is
// fixed. Currently nvcc fails in CI if this is inline and leaks to .cu files.
KATANA_EXPORT Result<void> ResultSuccess();
KATANA_EXPORT CopyableResult<void> CopyableResultSuccess();
// TODO(ddn): nvcc has trouble applying implicit conversions while other
// compilers can apply the implicit conversion from ErrorInfo to Result. Remove
// this function when nvcc improves.
KATANA_EXPORT Result<void> ResultError(ErrorInfo&& info);

inline std::error_code
ResultErrno() {
  KATANA_LOG_DEBUG_ASSERT(errno);
  return std::error_code(errno, std::system_category());
}

// Support functions for KATANA_CHECKED
namespace internal {

template <class T>
bool
CheckedExpressionFailed(const CopyableResult<T>& result) {
  return !result;
}

template <class T>
bool
CheckedExpressionFailed(const Result<T>& result) {
  return !result;
}

inline bool
CheckedExpressionFailed(const arrow::Status& status) {
  return !status.ok();
}

template <class T>
bool
CheckedExpressionFailed(const arrow::Result<T>& result) {
  return CheckedExpressionFailed(result.status());
}

template <class T>
ErrorInfo
CheckedExpressionToError(const Result<T>& result) {
  return result.error();
}

template <class T>
CopyableErrorInfo
CheckedExpressionToError(const CopyableResult<T>& result) {
  return result.error();
}

inline ErrorCode
ArrowStatusCodeToKatana(arrow::StatusCode code) {
  switch (code) {
  case arrow::StatusCode::Invalid:
    return ErrorCode::InvalidArgument;
  case arrow::StatusCode::TypeError:
    return ErrorCode::TypeError;
  case arrow::StatusCode::AlreadyExists:
    return ErrorCode::AlreadyExists;
  case arrow::StatusCode::KeyError:
  case arrow::StatusCode::IndexError:
    return ErrorCode::NotFound;
  default:
    return ErrorCode::ArrowError;
  }
}

inline ErrorInfo
CheckedExpressionToError(const arrow::Status& status) {
  ErrorCode code = ArrowStatusCodeToKatana(status.code());
  return ErrorInfo(code).WithContext("{}", status.message());
}

template <class T>
ErrorInfo
CheckedExpressionToError(const arrow::Result<T>& result) {
  return CheckedExpressionToError(result.status());
}

template <class T>
std::enable_if_t<!std::is_same<T, void>::value, T&&>
CheckedExpressionToValue(Result<T>&& result) {
  return std::move(result.value());
}

inline int
CheckedExpressionToValue(Result<void>&&) {
  return 0;
}

template <class T>
std::enable_if_t<!std::is_same<T, void>::value, T&&>
CheckedExpressionToValue(CopyableResult<T>&& result) {
  return std::move(result.value());
}

inline int
CheckedExpressionToValue(CopyableResult<void>&&) {
  return 0;
}

template <class T>
T&&
CheckedExpressionToValue(arrow::Result<T>&& result) {
  return std::move(result.ValueUnsafe());
}

inline int
CheckedExpressionToValue(arrow::Status&&) {
  return 0;
}

}  // namespace internal

#define KATANA_CHECKED_NAME(x, y) x##y

#define KATANA_CHECKED_IMPL(result_name, expression, ...)                      \
  ({                                                                           \
    auto result_name = (expression);                                           \
    if (::katana::internal::CheckedExpressionFailed(result_name)) {            \
      return ::katana::internal::CheckedExpressionToError(result_name)         \
          .WithContext(__VA_ARGS__)                                            \
          .WithContext("({}:{})", __FILE__, __LINE__);                         \
    }                                                                          \
    std::move(                                                                 \
        ::katana::internal::CheckedExpressionToValue(std::move(result_name))); \
  })

/// KATANA_CHECKED_CONTEXT takes an expression that returns a Result, and
/// additional error formatting expressions. If the Result has an error, the
/// function calling KATANA_CHECKED_CONTEXT will return the error with
/// additional error formatting. Otherwise, KATANA_CHECKED_CONTEXT will return
/// the value of the Result object to the caller.
#define KATANA_CHECKED_CONTEXT(expression, ...)                                \
  KATANA_CHECKED_IMPL(                                                         \
      KATANA_CHECKED_NAME(_error_or_value, __COUNTER__), expression,           \
      __VA_ARGS__)

/// KATANA_CHECKED takes an expression that returns a Result, and if the Result
/// has an error, the function calling KATANA_CHECKED will return the error.
/// Otherwise, KATANA_CHECKED will return the value of the Result object to the
/// caller.
#define KATANA_CHECKED(expression)                                             \
  KATANA_CHECKED_CONTEXT(expression, "backtrace")

}  // namespace katana

#endif

#ifndef KATANA_LIBSUPPORT_KATANA_RESULT_H_
#define KATANA_LIBSUPPORT_KATANA_RESULT_H_

#include <cerrno>
#include <cstring>
#include <ostream>
#include <system_error>

#include <boost/outcome/outcome.hpp>
#include <boost/outcome/utils.hpp>
#include <fmt/format.h>

#include "katana/Logging.h"
#include "katana/config.h"

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

/// An ErrorInfo contains additional context about an error in addition to an
/// error code. It works together with Result and user-defined error codes.
///
/// An example of use:
///
///   Result<One> MakeOne() {
///     if (...) {
///       // Return an error without any additional context
///       return ErrorCode::BadFoo;
///     } else if (...) {
///       // Return an error with context
///       return KATANA_ERROR(ErrorCode::BadFoo, "context message {}", ...);
///     }
///
///     return One();
///   }
///
///   Result<Many> MakeMany() {
///     for (...) {
///       auto r = MakeOne();
///       if (!r) {
///         if (r == ErrorCode::NoFoo) {
///           // Handle an error
///           continue;
///         }
///
///         // Propagate an error
///         return r.error().WithContext("making many");
///       }
///     }
///
///     return Many();
///   }
///
///   void User() {
///     auto r = MakeMany();
///     if (!r) {
///       std::cout << "error " << r.error() << "\n";
///       // or...
///       KATANA_LOG_ERROR("error {}", r.error());
///     }
///   }
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

  constexpr static int kContextSize = 512;

  ErrorInfo(const std::error_code& ec) : error_code_(ec) {}

  ErrorInfo() : ErrorInfo(std::error_code()) {}

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

  const std::error_code& error_code() const { return error_code_; }

  /// MakeWithSourceInfo makes an ErrorInfo from a root error with additional
  /// arguments passed to fmt::format
  template <typename F, typename... Args>
  static ErrorInfo MakeWithSourceInfo(
      const char* file_name, int line_no, const std::error_code& ec,
      F fmt_string, Args&&... args) {
    fmt::memory_buffer out;
    fmt::format_to(out, fmt_string, std::forward<Args>(args)...);
    const char* base_name = std::strrchr(file_name, '/');
    if (!base_name) {
      base_name = file_name;
    } else {
      base_name++;
    }

    fmt::format_to(out, " ({}:{})", base_name, line_no);

    ErrorInfo ei(ec);
    ei.Prepend(out.begin(), out.end());

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

  friend std::ostream& operator<<(std::ostream& out, const ErrorInfo& ei) {
    return ei.Write(out);
  }

  std::ostream& Write(std::ostream & out) const;

private:
  template <typename F, typename... Args>
  void PrependFmt(F fmt_string, Args && ... args) {
    fmt::memory_buffer out;
    fmt::format_to(out, fmt_string, std::forward<Args>(args)...);
    Prepend(out.begin(), out.end());
  }

  void Prepend(const char* begin, const char* end);

  /// SpillMessage writes the current error_code message to the error
  /// context if the error context is empty
  void SpillMessage();

  std::error_code error_code_;
  std::pair<Context*, int> context_{};
};

/// KATANA_ERROR creates new ErrorInfo and records information about the
/// callsite (e.g., line number).
#define KATANA_ERROR(ec, fmt_string, ...)                                      \
  ::katana::ErrorInfo::MakeWithSourceInfo(                                     \
      __FILE__, __LINE__, (ec), FMT_STRING(fmt_string), ##__VA_ARGS__);

/// A CopyableErrorInfo is a variant of ErrorInfo that can be used
/// outside a thread's error stack.
///
/// An ErrorInfo is not generally copyable because it references thread
/// local data. A CopyableErrorInfo is useful in cases where one wants
/// to store errors, e.g., collecting results across threads.
class KATANA_EXPORT CopyableErrorInfo {
public:
  CopyableErrorInfo(const ErrorInfo& ei);

  const std::error_code& error_code() const { return error_code_; }

  friend std::ostream& operator<<(
      std::ostream& out, const CopyableErrorInfo& ei) {
    return ei.Write(out);
  }

  std::ostream& Write(std::ostream& out) const;

private:
  std::error_code error_code_;
  std::string message_;
};

/// make_error_code converts ErrorInfo into a standard error code. It is an STL
/// and boost::outcome extension point and will be found with ADL if necessary.
inline std::error_code
make_error_code(ErrorInfo e) noexcept {
  return e.error_code();
}

/// A Result is a T or an ErrorInfo.
template <class T>
using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<
    T, ErrorInfo, internal::abort_policy>;

inline bool
operator==(const ErrorInfo& a, const ErrorInfo& b) {
  return make_error_code(a) == make_error_code(b);
}

inline bool
operator!=(const ErrorInfo& a, const ErrorInfo& b) {
  return !(a == b);
}

inline auto
ResultSuccess() {
  return BOOST_OUTCOME_V2_NAMESPACE::success();
}

inline std::error_code
ResultErrno() {
  KATANA_LOG_DEBUG_ASSERT(errno);
  return std::error_code(errno, std::system_category());
}

}  // namespace katana

#endif

#ifndef KATANA_LIBSUPPORT_KATANA_LOGGING_H_
#define KATANA_LIBSUPPORT_KATANA_LOGGING_H_

#include <mutex>
#include <sstream>
#include <string>
#include <system_error>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "katana/config.h"

/// Write debug, warning and error messages to standard error.
///
/// Warnings are for situations where code can proceed but in a suboptimal way.
///
/// Errors are for situations where code cannot proceed. If it is possible for
/// the caller to make progress, instead of using the log functions here, it is
/// preferable to return an explicit error via Result.h and let callers
/// determine what to do.
///
/// Messages at the debug level are only emitted in debug builds.
///
/// Messages at the warning and error level are commonly read by people not
/// familiar with the component producing the message, so at these levels,
/// avoid jargon and express error conditions using the general terms of the
/// system first (e.g., graphs, RDGs, properties) and add implementation
/// details afterwards if needed.
///
/// Could be better:
///
///     "type_class is null"
///
///     "type class ID {} > 3"
///
/// Preferred:
///
///     "cannot load graph {}: type class ID {} > 3"
///
/// Most of the logging functions take a format string, a C++20 STL feature,
/// that is implemented in our C++17 codebase with the fmt library.
///
/// An example of a format string:
///
///     KATANA_LOG_VERBOSE("hello {}", s);
///
/// In order to print a user-defined type, you can define the appropriate
/// ostream overload:
///
///     struct MyType {};
///
///     std::ostream& operator<<(std::ostream& out, MyType item);
///
/// It is also possible to directly specialize a user-defined type in the fmt
/// library, but this should be avoided because it is not compliant with the
/// C++20 STL interface and does not support traditional ostream usage.
///
/// There is a bug in fmt<8 with fmt::join(format_string, args...) when
/// elements of args only overload operator<<. This manifests as a compilation
/// error. To work around this, use katana::Join instead of fmt::join.
///
/// \file Logging.h

// Small patch to work with libfmt 4.0, which is the version in Ubuntu 18.04.
#ifndef FMT_STRING
#define FMT_STRING(...) __VA_ARGS__
#endif

#if FMT_VERSION >= 60000
/// Introduce std::error_code to the fmt library. Otherwise, they will be
/// printed using ostream<< formatting (i.e., as an int).
template <>
struct fmt::formatter<std::error_code> : formatter<string_view> {
  template <typename FormatterContext>
  auto format(std::error_code c, FormatterContext& ctx) {
    return formatter<string_view>::format(c.message(), ctx);
  }
};
#endif

namespace katana {

enum class LogLevel {
  Debug = 0,
  Verbose = 1,
  // Info = 2,  currently unused
  Warning = 3,
  Error = 4,
};

namespace internal {

KATANA_EXPORT void LogString(LogLevel level, const std::string& s);

}

/// Log at a specific LogLevel.
///
/// \tparam F         string-like type
/// \param level      level to log at
/// \param fmt_string a C++20-style fmt string (e.g., "hello {}")
/// \param args       arguments to fmt interpolation
template <typename F, typename... Args>
void
Log(LogLevel level, F fmt_string, Args&&... args) {
  std::string s = fmt::format(fmt_string, std::forward<Args>(args)...);
  internal::LogString(level, s);
}

/// Log at a specific LogLevel with source code information.
///
/// \tparam F         string-like type
/// \param level      level to log at
/// \param file_name  file name
/// \param line_no    line number
/// \param fmt_string a C++20-style fmt string (e.g., "hello {}")
/// \param args       arguments to fmt interpolation
template <typename F, typename... Args>
void
LogLine(
    LogLevel level, const char* file_name, int line_no, F fmt_string,
    Args&&... args) {
  std::string s = fmt::format(fmt_string, std::forward<Args>(args)...);
  std::string with_line = fmt::format("{}:{}: {}", file_name, line_no, s);
  internal::LogString(level, with_line);
}

KATANA_EXPORT void AbortApplication [[noreturn]] ();

}  // end namespace katana

/// KATANA_LOG_FATAL logs a message at the error log level and aborts the
/// application.
///
/// Use sparingly. It is usually preferable to return a katana::Result.
#define KATANA_LOG_FATAL(fmt_string, ...)                                      \
  do {                                                                         \
    ::katana::LogLine(                                                         \
        ::katana::LogLevel::Error, __FILE__, __LINE__, FMT_STRING(fmt_string), \
        ##__VA_ARGS__);                                                        \
    ::katana::AbortApplication();                                              \
  } while (0)
/// KATANA_LOG_ERROR logs a message at the error log level.
#define KATANA_LOG_ERROR(fmt_string, ...)                                      \
  do {                                                                         \
    ::katana::LogLine(                                                         \
        ::katana::LogLevel::Error, __FILE__, __LINE__, FMT_STRING(fmt_string), \
        ##__VA_ARGS__);                                                        \
  } while (0)
/// KATANA_LOG_WARN logs a message at the warning log level.
#define KATANA_LOG_WARN(fmt_string, ...)                                       \
  do {                                                                         \
    ::katana::LogLine(                                                         \
        ::katana::LogLevel::Warning, __FILE__, __LINE__,                       \
        FMT_STRING(fmt_string), ##__VA_ARGS__);                                \
  } while (0)
/// KATANA_LOG_VERBOSE logs a message at the verbose log level.
#define KATANA_LOG_VERBOSE(fmt_string, ...)                                    \
  do {                                                                         \
    ::katana::LogLine(                                                         \
        ::katana::LogLevel::Verbose, __FILE__, __LINE__,                       \
        FMT_STRING(fmt_string), ##__VA_ARGS__);                                \
  } while (0)

#ifndef NDEBUG
/// KATANA_LOG_DEBUG logs a message at the debug log level. Debug messages are
/// only produced in debug builds.
#define KATANA_LOG_DEBUG(fmt_string, ...)                                      \
  do {                                                                         \
    ::katana::LogLine(                                                         \
        ::katana::LogLevel::Debug, __FILE__, __LINE__, FMT_STRING(fmt_string), \
        ##__VA_ARGS__);                                                        \
  } while (0)
#else
#define KATANA_LOG_DEBUG(...)
#endif

/// KATANA_LOG_ASSERT asserts that a condition is true, and if it is not,
/// aborts the application.
#define KATANA_LOG_ASSERT(cond)                                                \
  do {                                                                         \
    if (!(cond)) {                                                             \
      ::katana::LogLine(                                                       \
          ::katana::LogLevel::Error, __FILE__, __LINE__,                       \
          "assertion not true: {}", #cond);                                    \
      ::katana::AbortApplication();                                            \
    }                                                                          \
  } while (0)

/// KATANA_LOG_VASSERT asserts that a condition is true, and if it is not, logs
/// an error and aborts the application.
#define KATANA_LOG_VASSERT(cond, fmt_string, ...)                              \
  do {                                                                         \
    if (!(cond)) {                                                             \
      ::katana::LogLine(                                                       \
          ::katana::LogLevel::Error, __FILE__, __LINE__,                       \
          FMT_STRING(fmt_string), ##__VA_ARGS__);                              \
      ::katana::AbortApplication();                                            \
    }                                                                          \
  } while (0)

/// KATANA_WARN_ONCE logs a message at the warning log level. The output of
/// subsequent invocations of KATANA_WARN_ONCE will be suppressed.
#define KATANA_WARN_ONCE(fmt_string, ...)                                      \
  do {                                                                         \
    static std::once_flag __katana_warn_once_flag;                             \
    std::call_once(__katana_warn_once_flag, [&]() {                            \
      ::katana::LogLine(                                                       \
          ::katana::LogLevel::Warning, __FILE__, __LINE__,                     \
          FMT_STRING(fmt_string), ##__VA_ARGS__);                              \
    });                                                                        \
  } while (0)

#ifndef NDEBUG
/// KATANA_LOG_ASSERT asserts that a condition is true, and if it is not,
/// aborts the application only in debug builds. This is a replacement for
/// std::assert.
#define KATANA_LOG_DEBUG_ASSERT(cond) KATANA_LOG_ASSERT(cond)

/// KATANA_LOG_VASSERT asserts that a condition is true, and if it is not, logs
/// an error and aborts the application only in debug builds. This is a
// replacement for std::assert.
#define KATANA_LOG_DEBUG_VASSERT(cond, fmt_string, ...)                        \
  KATANA_LOG_VASSERT(cond, fmt_string, ##__VA_ARGS__)

/// KATANA_DEBUG_WARN_ONCE logs a message at the warning log level only in
/// debug builds. The output of subsequent invocations of KATANA_WARN_ONCE will
/// be suppressed.
#define KATANA_DEBUG_WARN_ONCE(fmt_string, ...)                                \
  KATANA_WARN_ONCE(fmt_string, ##__VA_ARGS__)
#else
#define KATANA_LOG_DEBUG_ASSERT(cond)

#define KATANA_LOG_DEBUG_VASSERT(...)

#define KATANA_DEBUG_WARN_ONCE(...)
#endif

#endif

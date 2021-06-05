#include "katana/Result.h"

#include <array>

namespace {

struct Header {
  int size{};
};

}  // namespace

/// A Context is additional information attached to an ErrorInfo. It is
/// implemented by a fixed size thread local buffer.
///
/// To minimize TLS loads, the pointer to thread local data is stored in
/// ErrorInfo itself.
///
/// Because there is only one thread local buffer, but it is possible for users
/// to create multiple ErrorInfo objects at a time, we use a fat pointer
/// std::pair<Context*,int> to Context to check that uses of ErrorInfo are
/// linear (i.e., updates only happen at the most recent value). This check is
/// only best-effort. A more precise implementation would be to atomically
/// increment a version number.
///
/// Our most common operation is prepend, so Context::data_ grows from its end.
///
/// Doxygen does not like out-of-line nested structs
/// \cond DO_NOT_DOCUMENT
class katana::ErrorInfo::Context {
public:
  static constexpr int kDataSize =
      katana::ErrorInfo::kContextSize - sizeof(Header);

  const char* begin() const { return data_.end() - header_.size; }

  const char* end() const { return data_.end(); }

  char* begin() { return data_.end() - header_.size; }

  char* end() { return data_.end(); }

  void Prepend(const char* start, const char* end) {
    size_t remaining = data_.size() - header_.size;
    size_t len = std::distance(start, end);
    if (remaining < len) {
      start += len - remaining;
      len = remaining;
    }

    header_.size += len;

    std::copy(start, end, begin());
  }

  int size() const { return header_.size; }

  void reset() { header_.size = 0; }

private:
  Header header_;
  std::array<char, kDataSize> data_;
};

/// \endcond DO_NOT_DOCUMENT

namespace {

thread_local katana::ErrorInfo::Context kContext;

}  // namespace

static_assert(
    fmt::inline_buffer_size > katana::ErrorInfo::kContextSize / 2,
    "libfmt buffer size is small relative to max ErrorInfo context size");

inline void
katana::ErrorInfo::CheckContext() {
  // This assert can fail for a few reasons:
  // 1) an ErrorInfo or Result being passed from one thread to another,
  // 2) two ErrorInfos or Results being used at the same time on a thread,
  // 3) multiple copies of libsupport.
  KATANA_LOG_DEBUG_VASSERT(
      (!context_.first || context_.first == &kContext) &&
          (!context_.first || context_.first->size() == context_.second),
      "ErrorInfo object does not match thread-local ErrorInfo data. An "
      "ErrorInfo or Result is probably being misused. Probable original error: "
      "{}",
      *this);
}

void
katana::ErrorInfo::SpillMessage() {
  CheckContext();

  if (!context_.first) {
    std::string msg = error_code_.message();
    const char* start = msg.c_str();
    Prepend(start, start + msg.size());
  }
}

void
katana::ErrorInfo::Prepend(const char* begin, const char* end) {
  CheckContext();

  if (context_.first) {
    constexpr std::array<char, 2> kSep = {':', ' '};
    context_.first->Prepend(kSep.begin(), kSep.end());
  } else {
    context_.first = &kContext;
    context_.first->reset();
  }

  context_.first->Prepend(begin, end);
  context_.second = context_.first->size();
}

/// Doxygen cannot match this definition with its declaration in Result.h
/// \cond DO_NOT_DOCUMENT
// TODO(ddn): Revisit
std::ostream&
katana::ErrorInfo::Write(std::ostream& out) const {
  if ((context_.first && context_.first != &kContext) ||
      (context_.first && context_.first->size() != context_.second)) {
    KATANA_LOG_WARN(
        "ErrorInfo object does not match thread-local ErrorInfo data. Error "
        "messages may be corrupted.");
  }

  if (!context_.first) {
    out << error_code().message();
  } else {
    out << std::string_view(context_.first->begin(), context_.first->size());
  }

  return out;
}
/// \endcond DO_NOT_DOCUMENT

katana::CopyableErrorInfo::CopyableErrorInfo(const ErrorInfo& ei) {
  std::stringstream out;
  out << ei;

  error_code_ = ei.error_code();
  message_ = out.str();
}

std::ostream&
katana::CopyableErrorInfo::Write(std::ostream& out) const {
  if (message_.empty()) {
    out << error_code_.message();
  } else {
    out << message_;
  }
  return out;
}

katana::Result<void>
katana::ResultSuccess() {
  return BOOST_OUTCOME_V2_NAMESPACE::success();
}
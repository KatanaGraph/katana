#ifndef GALOIS_LIBSUPPORT_GALOIS_URI_H_
#define GALOIS_LIBSUPPORT_GALOIS_URI_H_

#include <string>
#include <string_view>

#include <fmt/format.h>

#include "galois/Result.h"
#include "galois/config.h"

namespace galois {

class GALOIS_EXPORT Uri {
  std::string scheme_;
  std::string path_;

  Uri(std::string scheme, std::string path);

public:
  Uri() = default;

  /// build a URI based on str. If no scheme is given, str is assumed to be a
  /// file path which and scheme is assumed to be `file://`
  static Result<Uri> Make(const std::string& str);
  static Result<Uri> MakeFromFile(const std::string& str);

  std::string Encode() const {
    return fmt::format("backend-{}/{}", scheme_, path_);
  }

  std::string_view scheme() const { return scheme_; }
  std::string_view path() const { return path_; }

  std::string string() const {
    if (empty()) {
      return "";
    }
    return scheme_ + "://" + path_;
  }

  bool empty() const {
    if (scheme_.empty()) {
      assert(path_.empty());
      return true;
    }
    return false;
  }

  // it's convenient to treat URIs like paths sometimes
  Uri DirName() const;
  std::string BaseName() const;
  Uri Append(std::string_view to_append) const;
  /// generate a new uri that is this uri with `prefix-XXXXX` appended where
  /// XXXX is a random alpha numeric string
  Uri RandFile(std::string_view prefix) const;
};

GALOIS_EXPORT bool operator==(const Uri& lhs, const Uri& rhs);
GALOIS_EXPORT bool operator!=(const Uri& lhs, const Uri& rhs);

}  // namespace galois

template <>
struct GALOIS_EXPORT fmt::formatter<galois::Uri> : formatter<std::string> {
  template <typename FormatContext>
  auto format(const galois::Uri& uri, FormatContext& ctx) {
    return formatter<std::string>::format(uri.string(), ctx);
  }
};

#endif

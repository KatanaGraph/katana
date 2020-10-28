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
  std::string string_;

  Uri(std::string scheme, std::string path);

public:
  static constexpr char kSepChar = '/';
  Uri() = default;

  /// build a URI based on str. If no scheme is given, str is assumed to be a
  /// file path which and scheme is assumed to be `file://`
  static Result<Uri> Make(const std::string& str);
  static Result<Uri> MakeFromFile(const std::string& str);
  /// Append a '-' and then a random string to input
  static Result<Uri> MakeRand(const std::string& str);
  static std::string JoinPath(const std::string& dir, const std::string& file);

  /// Return the base64 (url variant) encoded version of this uri
  std::string Encode() const;

  const std::string& scheme() const { return scheme_; }
  const std::string& path() const { return path_; }
  const std::string& string() const { return string_; }

  bool empty() const;

  // it's convenient to treat URIs like paths sometimes
  Uri DirName() const;
  std::string BaseName() const;
  // Join new component with a kSepChar
  Uri Join(std::string_view to_join) const;
  Uri StripSep() const;

  /// generate a new uri that is this uri with `prefix-XXXXX` appended where
  /// XXXX is a random alpha numeric string
  Uri RandFile(std::string_view prefix) const;

  GALOIS_EXPORT friend Uri operator+(const Uri& lhs, char rhs);
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

#ifndef KATANA_LIBSUPPORT_KATANA_URI_H_
#define KATANA_LIBSUPPORT_KATANA_URI_H_

#include <string>
#include <string_view>

#include <fmt/format.h>

#include "katana/Result.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT Uri {
public:
  static constexpr const char kSepChar = '/';
  static constexpr const char* kFileScheme = "file";

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
  /// Hash URI
  struct Hash {
    std::size_t operator()(const Uri& uri) const {
      return std::hash<std::string>{}(uri.path_);
    }
  };

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

  Uri operator+(char rhs) const;
  Uri operator+(const std::string& rhs) const;

private:
  Uri(std::string scheme, std::string path);

  std::string scheme_;
  std::string path_;
  std::string string_;
};

KATANA_EXPORT bool operator==(const Uri& lhs, const Uri& rhs);
KATANA_EXPORT bool operator!=(const Uri& lhs, const Uri& rhs);

}  // namespace katana

template <>
struct KATANA_EXPORT fmt::formatter<katana::Uri> : formatter<std::string> {
  template <typename FormatContext>
  auto format(const katana::Uri& uri, FormatContext& ctx) {
    return formatter<std::string>::format(uri.string(), ctx);
  }
};

#endif

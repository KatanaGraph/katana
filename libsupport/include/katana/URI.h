#ifndef KATANA_LIBSUPPORT_KATANA_URI_H_
#define KATANA_LIBSUPPORT_KATANA_URI_H_

#include <string>
#include <string_view>

#include <fmt/format.h>

#include "katana/Result.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT URI {
public:
  static constexpr const char kSepChar = '/';
  static constexpr const char* kFileScheme = "file";

  URI() = default;

  /// build a URI based on str. If no scheme is given, str is assumed to be a
  /// file path which and scheme is assumed to be `file://`
  static Result<URI> Make(const std::string& str);
  static Result<URI> MakeFromFile(const std::string& str);
  /// Append a '-' and then a random string to input
  static Result<URI> MakeRand(const std::string& str);
  /// Make a URI for a local user-configurable temporary directory
  /// The URI will encode "/tmp" unless one of the following environment
  /// variables is set (later list entries overrule previous entries):
  /// 1. TMP
  /// 2. TMPDIR
  /// 3. KATANA_TMPDIR
  /// The contents of the "most senior" set variable from this list will be used
  /// if any are set.
  static Result<URI> MakeTempDir();

  static std::string JoinPath(const std::string& dir, const std::string& file);

  // Decode returns the raw bytes represented by a URI encoded string.
  static std::string Decode(const std::string& uri);

  /// Hash URI
  struct Hash {
    std::size_t operator()(const URI& uri) const {
      return std::hash<std::string>{}(uri.path_);
    }
  };

  const std::string& scheme() const { return scheme_; }

  /// path returns the portion of a URI after the scheme. This a concatenation
  /// of the traditional URI host and path components. Unlike string(), the
  /// returned value are raw bytes and there is no encoding of special
  /// characters.
  const std::string& path() const { return path_; }

  /// string returns the URI as a URI-encoded string.
  const std::string& string() const { return encoded_; }

  bool empty() const;

  // it's convenient to treat URIs like paths sometimes
  URI DirName() const;
  std::string BaseName() const;
  // Join new component with a kSepChar
  URI Join(std::string_view to_join) const;
  URI StripSep() const;

  /// generate a new uri that is this uri with `prefix-XXXXX` appended where
  /// XXXX is a random alpha numeric string
  URI RandFile(std::string_view prefix) const;
  /// An overload of RandFile provided for clarity at call sites
  URI RandSubdir(std::string_view prefix) const { return RandFile(prefix); }

  URI operator+(char rhs) const;
  URI operator+(const std::string& rhs) const;

private:
  URI(std::string scheme, std::string path);

  std::string scheme_;
  std::string path_;
  std::string encoded_;
};

KATANA_EXPORT bool operator==(const URI& lhs, const URI& rhs);
KATANA_EXPORT bool operator!=(const URI& lhs, const URI& rhs);

[[deprecated("use URI")]] typedef URI Uri;

}  // namespace katana

template <>
struct KATANA_EXPORT fmt::formatter<katana::URI> : formatter<std::string> {
  template <typename FormatContext>
  auto format(const katana::URI& uri, FormatContext& ctx) {
    return formatter<std::string>::format(uri.string(), ctx);
  }
};

#endif

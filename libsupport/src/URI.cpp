#include "katana/URI.h"

#include <libgen.h>

#include <array>
#include <climits>
#include <cstdlib>
#include <regex>

#include <fmt/format.h>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Random.h"
#include "katana/Result.h"
#include "katana/Strings.h"

namespace {

// get scheme and path, always drop trailing slash
const std::regex kUriRegex("^(?:([a-zA-Z0-9]+)://)?(.+?)/?$");

// This function does not recognize any path seperator other than kSepChar. This
// could be a problem for Windows or "non-standard S3" paths.
std::string
ExtractFileName(std::string_view path) {
  size_t last_slash =
      path.find_last_of(katana::Uri::kSepChar, std::string::npos);
  size_t name_end_plus1 = path.length();
  if (last_slash == std::string::npos) {
    return std::string(path);
  }
  if (last_slash == (path.length() - 1)) {
    // Find end of file name
    while (last_slash > 0 && path[last_slash] == katana::Uri::kSepChar) {
      last_slash--;
    }
    name_end_plus1 =
        last_slash + 1;  // name_end_plus1 points to last slash in group
    while (last_slash > 0 && path[last_slash] != katana::Uri::kSepChar) {
      last_slash--;
    }
  }
  return std::string(
      path.substr(last_slash + 1, name_end_plus1 - last_slash - 1));
}

// Use same rules as python's os.path.dirname
std::string
ExtractDirName(std::string_view path) {
  size_t last_slash =
      path.find_last_of(katana::Uri::kSepChar, std::string::npos);
  if (last_slash == std::string::npos) {
    return "";
  }
  if (last_slash == (path.length() - 1)) {
    // Find end of file name
    while (last_slash > 0 && path[last_slash] == katana::Uri::kSepChar) {
      last_slash--;
    }
    // Find first slash before file name
    while (last_slash > 0 && path[last_slash] != katana::Uri::kSepChar) {
      last_slash--;
    }
    // Find first slash after directory name
    while (last_slash > 0 && path[last_slash] == katana::Uri::kSepChar) {
      last_slash--;
    }
    last_slash++;
  }
  return std::string(path.substr(0, last_slash));
}

std::string
AddRandComponent(const std::string& str) {
  std::string name(str);
  name += "-";
  name += katana::RandomAlphanumericString(12);
  return name;
}

std::string
NewPath(std::string_view dir, std::string_view prefix) {
  std::string name(prefix);
  if (prefix.front() == katana::Uri::kSepChar) {
    name = name.substr(1, std::string::npos);
  }
  name = AddRandComponent(name);
  std::string p{dir};
  if (p.back() == katana::Uri::kSepChar) {
    p = p.substr(0, p.length() - 1);
  }
  return p.append(1, katana::Uri::kSepChar).append(name);
}

std::string
DoJoinPath(std::string_view dir, std::string_view file) {
  if (dir.empty()) {
    return std::string(file);
  }
  if (dir[dir.size() - 1] != katana::Uri::kSepChar) {
    if (file[0] != katana::Uri::kSepChar) {
      return fmt::format("{}{}{}", dir, katana::Uri::kSepChar, file);
    }
    while (file[1] == katana::Uri::kSepChar) {
      file.remove_prefix(1);
    }
    return fmt::format("{}{}", dir, file);
  }
  while (dir[dir.size() - 2] == katana::Uri::kSepChar) {
    dir.remove_suffix(1);
  }
  while (file[0] == katana::Uri::kSepChar) {
    file.remove_prefix(1);
  }
  KATANA_LOG_ASSERT(dir[dir.size() - 1] == katana::Uri::kSepChar);
  return fmt::format("{}{}", dir, file);
}

bool
ShouldURLEncode(int c) {
  if ('a' <= c && c <= 'z') {
    return false;
  }
  if ('A' <= c && c <= 'Z') {
    return false;
  }
  if ('0' <= c && c <= '9') {
    return false;
  }
  if (c == '-') {
    return false;
  }
  if (c == '.') {
    return false;
  }
  if (c == '_') {
    return false;
  }
  if (c == '~') {
    return false;
  }

  // We encode whole paths, so in addition to the standard unencoded characters
  // above, we should not encode '/' either.
  if (c == '/') {
    return false;
  }

  return true;
}

// ToHex converts a char between 0 and 15 to an ASCII character from 0 to F.
char
ToHex(int c) {
  c = c & 0x0F;

  if (c < 10) {
    return '0' + c;
  }
  return 'A' + (c - 10);
}

int
FromHex(char a) {
  if ('0' <= a && a <= '9') {
    return a - '0';
  }
  if ('A' <= a && a <= 'F') {
    return a - 'A' + 10;
  }
  return 0;
}

std::string
URLEncode(const std::string& s) {
  std::vector<char> buf;

  for (const char& c : s) {
    if (!ShouldURLEncode(c)) {
      buf.emplace_back(c);
      continue;
    }

    buf.emplace_back('%');
    buf.emplace_back(ToHex(c >> 4));
    buf.emplace_back(ToHex(c & 0x0F));
  }

  return std::string(buf.data(), buf.size());
}

std::string
URLDecode(const std::string& s) {
  std::vector<char> buf;

  for (size_t i = 0, n = s.size(); i < n; ++i) {
    char c = s[i];
    if (c != '%') {
      buf.emplace_back(c);
      continue;
    }
    if (i + 2 >= n) {
      // invalid escape
      continue;
    }
    int a = FromHex(s[i + 1]);
    int b = FromHex(s[i + 2]);

    buf.emplace_back((a << 4) + b);
    i += 2;
  }

  return std::string(buf.data(), buf.size());
}

}  // namespace

namespace katana {

Uri::Uri(std::string scheme, std::string path)
    : scheme_(std::move(scheme)), path_(std::move(path)) {
  encoded_ = scheme_ + "://" + URLEncode(path_);

  KATANA_LOG_DEBUG_ASSERT(!scheme_.empty());
  KATANA_LOG_DEBUG_ASSERT(!path_.empty());
}

Result<Uri>
Uri::MakeFromFile(const std::string& str) {
  std::vector<char> path(PATH_MAX + 1);
  if (realpath(str.c_str(), path.data()) == nullptr) {
    return Uri(kFileScheme, str);
  }
  return Uri(kFileScheme, path.data());
}

Result<Uri>
Uri::Make(const std::string& str) {
  std::smatch sub_match;
  if (!std::regex_match(str, sub_match, kUriRegex)) {
    if (str.empty()) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument, "can't make URI from empty string");
    }
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "could not parse URI: {}", str);
  }
  std::string scheme(sub_match[1]);
  std::string path(sub_match[2]);
  if (scheme.empty()) {
    return MakeFromFile(path);
  }

  return Uri(scheme, URLDecode(path));
}

Result<Uri>
Uri::MakeRand(const std::string& str) {
  auto res = Make(AddRandComponent(str));
  if (!res) {
    return res.error();
  }
  return res.value();
}

std::string
Uri::JoinPath(const std::string& dir, const std::string& file) {
  return DoJoinPath(dir, file);
}

std::string
Uri::Decode(const std::string& uri) {
  return URLDecode(uri);
}

bool
Uri::empty() const {
  if (scheme_.empty()) {
    KATANA_LOG_DEBUG_ASSERT(path_.empty());
    return true;
  }
  return false;
}

std::string
Uri::BaseName() const {
  return ExtractFileName(path());
}

Uri
Uri::DirName() const {
  return Uri(scheme_, ExtractDirName(path()));
}

Uri
Uri::Join(std::string_view to_join) const {
  if (empty()) {
    return Uri();
  }
  return Uri(scheme_, DoJoinPath(path_, to_join));
}

Uri
Uri::StripSep() const {
  std::string path = path_;
  while (path[path.size() - 1] == kSepChar) {
    path.pop_back();
  }
  return Uri(scheme_, path);
}

Uri
Uri::RandFile(std::string_view prefix) const {
  if (empty()) {
    return Uri();
  }
  return Uri(scheme_, NewPath(path_, prefix));
}

bool
operator==(const Uri& lhs, const Uri& rhs) {
  return (lhs.scheme() == rhs.scheme()) && (lhs.path() == rhs.path());
}

bool
operator!=(const Uri& lhs, const Uri& rhs) {
  return !(lhs == rhs);
}

Uri
Uri::operator+(char rhs) const {
  return Uri(scheme_, path_ + rhs);
}

Uri
Uri::operator+(const std::string& rhs) const {
  return Uri(scheme_, path_ + rhs);
}

}  // namespace katana

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

}  // namespace

namespace katana {

Uri::Uri(std::string scheme, std::string path)
    : scheme_(std::move(scheme)),
      path_(std::move(path)),
      string_(scheme_ + "://" + path_) {
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
    return KATANA_ERROR(ErrorCode::InvalidArgument, "could not parse URI");
  }
  std::string scheme(sub_match[1]);
  std::string path(sub_match[2]);
  if (scheme.empty()) {
    return MakeFromFile(path);
  }
  return Uri(scheme, path);
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

bool
Uri::empty() const {
  if (scheme_.empty()) {
    KATANA_LOG_DEBUG_ASSERT(path_.empty());
    return true;
  }
  return false;
}

std::string
Uri::Encode() const {
  return katana::ToBase64(string(), true);
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

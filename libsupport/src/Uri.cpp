#include "galois/Uri.h"

#include <libgen.h>

#include <array>
#include <climits>
#include <cstdlib>
#include <regex>

#include <fmt/format.h>

#include "galois/ErrorCode.h"
#include "galois/Logging.h"
#include "galois/Random.h"
#include "galois/Result.h"

namespace {

// get scheme and path, always drop trailing slash
const std::regex kUriRegex("(?:([a-zA-Z0-9]+)://)?(.+)/?");
constexpr const char* kFileScheme = "file";

// base64 based on https://web.stanford.edu/class/archive/cs/cs106b/cs106b.1186/lectures/08-Fractals/code/expressions/lib/StanfordCPPLib/io/base64.cpp
const std::array<char, 64> kBase64Alphabet{
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'};

uint64_t
Base64EncodeLen(uint64_t len) {
  return ((len + 2) / 3 * 4);
}

std::string
Base64Encode(std::string_view s) {
  std::string res(Base64EncodeLen(s.length()), '=');
  uint64_t j = 0;
  uint64_t i;
  for (i = 0; i < s.length() - 2; i += 3) {
    res[j++] = kBase64Alphabet.at((s[i] >> 2) & 0x3F);
    res[j++] =
        kBase64Alphabet.at(((s[i] & 0x3) << 4) | ((s[i + 1] & 0xF0) >> 4));
    res[j++] =
        kBase64Alphabet.at(((s[i + 1] & 0xF) << 2) | ((s[i + 2] & 0xC0) >> 6));
    res[j++] = kBase64Alphabet.at(s[i + 2] & 0x3F);
  }
  if (i < s.length()) {
    res[j++] = kBase64Alphabet.at((s[i] >> 2) & 0x3F);
    if (i == (s.length() - 1)) {
      res[j++] = kBase64Alphabet.at(((s[i] & 0x3) << 4));
    } else {
      res[j++] =
          kBase64Alphabet.at(((s[i] & 0x3) << 4) | ((s[i + 1] & 0xF0) >> 4));
      res[j++] = kBase64Alphabet.at(((s[i + 1] & 0xF) << 2));
    }
    // array is initialized to pad character so no need to do that here
  }
  return res;
}

// This function does not recognize any path seperator other than kSepChar. This
// could be a problem for Windows or "non-standard S3" paths.
std::string
ExtractFileName(std::string_view path) {
  size_t last_slash =
      path.find_last_of(galois::Uri::kSepChar, std::string::npos);
  size_t name_end_plus1 = path.length();
  if (last_slash == std::string::npos) {
    return std::string(path);
  }
  if (last_slash == (path.length() - 1)) {
    // Find end of file name
    while (last_slash > 0 && path[last_slash] == galois::Uri::kSepChar) {
      last_slash--;
    }
    name_end_plus1 =
        last_slash + 1;  // name_end_plus1 points to last slash in group
    while (last_slash > 0 && path[last_slash] != galois::Uri::kSepChar) {
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
      path.find_last_of(galois::Uri::kSepChar, std::string::npos);
  if (last_slash == std::string::npos) {
    return "";
  }
  if (last_slash == (path.length() - 1)) {
    // Find end of file name
    while (last_slash > 0 && path[last_slash] == galois::Uri::kSepChar) {
      last_slash--;
    }
    // Find first slash before file name
    while (last_slash > 0 && path[last_slash] != galois::Uri::kSepChar) {
      last_slash--;
    }
    // Find first slash after directory name
    while (last_slash > 0 && path[last_slash] == galois::Uri::kSepChar) {
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
  name += galois::RandomAlphanumericString(12);
  return name;
}

std::string
NewPath(std::string_view dir, std::string_view prefix) {
  std::string name(prefix);
  if (prefix.front() == galois::Uri::kSepChar) {
    name = name.substr(1, std::string::npos);
  }
  name = AddRandComponent(name);
  std::string p{dir};
  if (p.back() == galois::Uri::kSepChar) {
    p = p.substr(0, p.length() - 1);
  }
  return p.append(1, galois::Uri::kSepChar).append(name);
}

std::string
DoJoinPath(std::string_view dir, std::string_view file) {
  if (dir.empty()) {
    return std::string(file);
  }
  if (dir[dir.size() - 1] != galois::Uri::kSepChar) {
    if (file[0] != galois::Uri::kSepChar) {
      return fmt::format("{}{}{}", dir, galois::Uri::kSepChar, file);
    } else {
      while (file[1] == galois::Uri::kSepChar) {
        file.remove_prefix(1);
      }
      return fmt::format("{}{}", dir, file);
    }
  } else {
    while (dir[dir.size() - 2] == galois::Uri::kSepChar) {
      dir.remove_suffix(1);
    }
    while (file[0] == galois::Uri::kSepChar) {
      file.remove_prefix(1);
    }
  }
  GALOIS_LOG_ASSERT(dir[dir.size() - 1] == galois::Uri::kSepChar);
  return fmt::format("{}{}", dir, file);
}

}  // namespace

namespace galois {

Uri::Uri(std::string scheme, std::string path)
    : scheme_(std::move(scheme)),
      path_(std::move(path)),
      string_(scheme_ + "://" + path_) {
  assert(!scheme_.empty());
  assert(!path_.empty());
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
    return ErrorCode::InvalidArgument;
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
    assert(path_.empty());
    return true;
  }
  return false;
}

std::string
Uri::Encode() const {
  return Base64Encode(string());
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
operator+(const Uri& lhs, char rhs) {
  return Uri(lhs.scheme_, lhs.path_ + rhs);
}

}  // namespace galois

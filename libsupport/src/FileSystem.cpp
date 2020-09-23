#include "galois/FileSystem.h"

#include <unistd.h>

#include <string_view>
#include <vector>

#include <boost/outcome/outcome.hpp>
#include <fmt/core.h>

#include "galois/ErrorCode.h"
#include "galois/Random.h"

static const std::string_view kExes = "XXXXXX";
static const char kSepChar = '/';

static std::vector<char>
TemplateString(std::string_view pre, std::string_view suf) {
  std::vector<char> res(pre.begin(), pre.end());
  res.insert(res.end(), kExes.begin(), kExes.end());
  res.insert(res.end(), suf.begin(), suf.end());
  res.emplace_back('\0');
  return res;
}

galois::Result<std::string>
galois::CreateUniqueFile(std::string_view prefix, std::string_view suffix) {
  auto result = OpenUniqueFile(prefix, suffix);
  if (!result) {
    return result.error();
  }
  auto [name, fd] = result.value();
  close(fd);
  return name;
}

galois::Result<std::pair<std::string, int>>
galois::OpenUniqueFile(std::string_view prefix, std::string_view suffix) {
  std::vector<char> buf(TemplateString(prefix, suffix));

  int fd = mkstemps(buf.data(), suffix.length());
  if (fd < 0) {
    return galois::ResultErrno();
  }

  return std::make_pair(std::string(buf.begin(), buf.end() - 1), fd);
}

galois::Result<std::string>
galois::CreateUniqueDirectory(std::string_view prefix) {
  std::vector<char> buf(TemplateString(prefix, ""));

  char* ret = mkdtemp(buf.data());
  if (ret == nullptr) {
    return std::error_code(errno, std::system_category());
  }

  return std::string(buf.begin(), buf.end() - 1);
}

std::string
galois::NewPath(std::string_view dir, std::string_view prefix) {
  std::string name(prefix);
  if (prefix.front() == kSepChar) {
    name = name.substr(1, std::string::npos);
  }
  name += "-";
  name += RandomAlphanumericString(12);
  std::string p{dir};
  if (p.back() == kSepChar) {
    p = p.substr(0, p.length() - 1);
  }
  return p.append(1, kSepChar).append(name);
}

// This function does not recognize any path seperator other than '/'. This
// could be a problem for Windows or "non-standard S3" paths.
std::string
galois::ExtractFileName(std::string_view path) {
  size_t last_slash = path.find_last_of(kSepChar, std::string::npos);
  size_t name_end_plus1 = path.length();
  if (last_slash == std::string::npos) {
    return std::string(path);
  }
  if (last_slash == (path.length() - 1)) {
    // Find end of file name
    while (last_slash > 0 && path[last_slash] == kSepChar) {
      last_slash--;
    }
    name_end_plus1 =
        last_slash + 1;  // name_end_plus1 points to last slash in group
    while (last_slash > 0 && path[last_slash] != kSepChar) {
      last_slash--;
    }
  }
  return std::string(
      path.substr(last_slash + 1, name_end_plus1 - last_slash - 1));
}

galois::Result<std::string>
galois::ExtractDirName(std::string_view path) {
  size_t last_slash = path.find_last_of(kSepChar, std::string::npos);
  if (last_slash == std::string::npos) {
    return ErrorCode::InvalidArgument;
  }
  if (last_slash == (path.length() - 1)) {
    // Find end of file name
    while (last_slash > 0 && path[last_slash] == kSepChar) {
      last_slash--;
    }
    // Find first slash before file name
    while (last_slash > 0 && path[last_slash] != kSepChar) {
      last_slash--;
    }
    // Find first slash after directory name
    while (last_slash > 0 && path[last_slash] == kSepChar) {
      last_slash--;
    }
    last_slash++;
  }
  return std::string(path.substr(0, last_slash));
}

std::string
galois::StripURIScheme(const std::string& uri) {
  size_t double_slash = uri.find("//");
  if (double_slash == std::string::npos) {
    return uri;
  }
  return uri.substr(double_slash + 2);
}

std::string
galois::JoinPath(std::string_view dir, std::string_view file) {
  size_t last_slash = dir.find_last_of(kSepChar, std::string::npos);
  if (last_slash == (dir.length() - 1)) {
    return fmt::format("{}{}", dir, file);
  }
  return fmt::format("{}{}{}", dir, kSepChar, file);
}

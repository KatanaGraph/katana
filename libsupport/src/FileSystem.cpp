#include "galois/FileSystem.h"

#include <string_view>
#include <vector>

#include <boost/outcome/outcome.hpp>
#include <unistd.h>

static const std::string_view kExes = "XXXXXX";

static std::vector<char> TemplateString(std::string_view pre,
                                        std::string_view suf) {
  std::vector<char> res(pre.begin(), pre.end());
  res.insert(res.end(), kExes.begin(), kExes.end());
  res.insert(res.end(), suf.begin(), suf.end());
  res.emplace_back('\0');
  return res;
}

galois::Result<std::string> galois::CreateUniqueFile(std::string_view prefix,
                                                     std::string_view suffix) {
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

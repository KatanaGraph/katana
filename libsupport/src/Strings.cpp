#include "katana/Strings.h"

namespace {

size_t
next_sep(std::string_view s, std::string_view sep, size_t start = 0) {
  if (sep.empty()) {
    return start + 1;
  }
  return s.find(sep, start);
}

}  // namespace

bool
katana::HasPrefix(const std::string& s, const std::string& prefix) {
  size_t prefix_len = prefix.length();
  size_t s_len = s.length();

  if (prefix_len > s_len) {
    return false;
  }

  return s.compare(0, prefix_len, prefix) == 0;
}

std::string
katana::TrimPrefix(const std::string& s, const std::string& prefix) {
  if (HasPrefix(s, prefix)) {
    size_t prefix_len = prefix.length();
    return s.substr(prefix_len, s.length() - prefix_len);
  }
  return s;
}

bool
katana::HasSuffix(const std::string& s, const std::string& suffix) {
  size_t suffix_len = suffix.length();
  size_t s_len = s.length();

  if (suffix_len > s_len) {
    return false;
  }

  return s.compare(s_len - suffix_len, suffix_len, suffix) == 0;
}

std::string
katana::TrimSuffix(const std::string& s, const std::string& suffix) {
  if (HasSuffix(s, suffix)) {
    size_t suffix_len = suffix.length();
    return s.substr(0, s.length() - suffix_len);
  }
  return s;
}

std::vector<std::string_view>
katana::SplitView(std::string_view s, std::string_view sep, uint64_t max) {
  std::vector<std::string_view> words;
  size_t start = 0;
  size_t end = next_sep(s, sep);
  for (uint64_t i = 0; i < max; ++i) {
    if (end >= s.length()) {
      break;
    }
    words.emplace_back(s.substr(start, end - start));
    start = end + sep.length();
    end = next_sep(s, sep, start);
  }
  words.emplace_back(s.substr(start));
  return words;
}

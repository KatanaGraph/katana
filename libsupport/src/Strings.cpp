#include "galois/Strings.h"

bool
galois::HasPrefix(const std::string& s, const std::string& prefix) {
  size_t prefix_len = prefix.length();
  size_t s_len = s.length();

  if (prefix_len > s_len) {
    return false;
  }

  return s.compare(0, prefix_len, prefix) == 0;
}

std::string
galois::TrimPrefix(const std::string& s, const std::string& prefix) {
  if (HasPrefix(s, prefix)) {
    size_t prefix_len = prefix.length();
    return s.substr(prefix_len, s.length() - prefix_len);
  }
  return s;
}

bool
galois::HasSuffix(const std::string& s, const std::string& suffix) {
  size_t suffix_len = suffix.length();
  size_t s_len = s.length();

  if (suffix_len > s_len) {
    return false;
  }

  return s.compare(s_len - suffix_len, suffix_len, suffix) == 0;
}

std::string
galois::TrimSuffix(const std::string& s, const std::string& suffix) {
  if (HasSuffix(s, suffix)) {
    size_t suffix_len = suffix.length();
    return s.substr(0, s.length() - suffix_len);
  }
  return s;
}

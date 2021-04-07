#include "katana/Strings.h"

#include <sstream>

#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/insert_linebreaks.hpp>
#include <boost/archive/iterators/remove_whitespace.hpp>
#include <boost/archive/iterators/transform_width.hpp>

namespace {

size_t
NextSep(std::string_view s, std::string_view sep, size_t start = 0) {
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
  size_t end = NextSep(s, sep);
  for (uint64_t i = 0; i < max; ++i) {
    if (end >= s.length()) {
      break;
    }
    words.emplace_back(s.substr(start, end - start));
    start = end + sep.length();
    end = NextSep(s, sep, start);
  }
  words.emplace_back(s.substr(start));
  return words;
}

std::string
katana::FromBase64(const std::string& input) {
  namespace bai = boost::archive::iterators;
  using BinTest = bai::transform_width<
      bai::binary_from_base64<std::string::const_iterator>, 8, 6>;
  auto binary = std::string(BinTest(input.begin()), BinTest(input.end()));
  // Remove padding.
  auto length = input.size();
  if (binary.size() > 2 && input[length - 1] == '=' &&
      input[length - 2] == '=') {
    binary.erase(binary.end() - 2, binary.end());
  } else if (binary.size() > 1 && input[length - 1] == '=') {
    binary.erase(binary.end() - 1, binary.end());
  }
  return binary;
}

std::string
katana::ToBase64(const std::string& msg, bool url_safe) {
  namespace bai = boost::archive::iterators;
  using Base64Text = bai::base64_from_binary<
      bai::transform_width<std::string::const_iterator, 6, 8>>;
  std::string b64 =
      std::string(Base64Text(msg.cbegin()), Base64Text(msg.cend()));
  // Replace the '+' and '/' symbols to ensure encoding is URL safe
  if (url_safe) {
    std::replace(b64.begin(), b64.end(), '+', '-');
    std::replace(b64.begin(), b64.end(), '/', '_');
  }
  // Add padding.
  return b64.append((3 - msg.size() % 3) % 3, '=');
}

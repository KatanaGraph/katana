#include "galois/FileSystem.h"

#include <string_view>
#include <vector>
#include <random>

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

// https://stackoverflow.com/questions/440133
template <typename T = std::mt19937>
auto random_generator() -> T {
  auto constexpr seed_bits = sizeof(typename T::result_type) * T::state_size;
  auto constexpr seed_len =
      seed_bits / std::numeric_limits<std::seed_seq::result_type>::digits;
  auto seed = std::array<std::seed_seq::result_type, seed_len>{};
  auto dev  = std::random_device{};
  std::generate_n(begin(seed), seed_len, std::ref(dev));
  auto seed_seq = std::seed_seq(begin(seed), end(seed));
  return T{seed_seq};
}

std::string generate_random_alphanumeric_string(std::size_t len) {
  static constexpr auto chars = "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";
  thread_local auto rng = random_generator<>();
  auto dist   = std::uniform_int_distribution{{}, std::strlen(chars) - 1};
  auto result = std::string(len, '\0');
  std::generate_n(begin(result), len, [&]() { return chars[dist(rng)]; });
  return result;
}

/// NewPath returns a new path in a directory with the given prefix. It works
/// by appending a random suffix. The generated paths are may not be unique due
/// to the varying atomicity guarantees of future storage backends.
// TODO (witchel) return galois::Result<std::string>
boost::filesystem::path galois::NewPath(const boost::filesystem::path& dir,
                                        const std::string& prefix) {
  std::string name = prefix;
  name += "-";
  name += generate_random_alphanumeric_string(12);
  boost::filesystem::path p{dir};
  return p.append(name);
}

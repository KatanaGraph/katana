// Test every libtsuba::file.h routine except get/put async functions which are tested by tsuba_bench
#include "tsuba/tsuba.h"

#include <getopt.h>

#include <vector>

#include "bench_utils.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "galois/Random.h"
#include "md5.h"
#include "tsuba/file.h"

// remove_all (rm -rf) is just too sweet
#include <boost/filesystem.hpp>

bool self_configure = true;
int opt_verbose_level{0};
std::string local_uri = "/tmp/tsuba_basic_quick-";
std::string remote_uri{};
std::string prog_name = "tsuba_basic_quick";
std::string usage_msg =
    "Usage: {} [options] <remote uri directory>\n"
    "  [--no-self-configure]\n"
    "  [-v] verbose, can be repeated (default=false)\n"
    "  [-h] usage message\n";

void
ParseArguments(int argc, char* argv[]) {
  int c;
  int option_index = 0;
  while (1) {
    static struct option long_options[] = {
        {"no-self-configure", no_argument, 0, 's'}, {0, 0, 0, 0}};
    c = getopt_long(argc, argv, "vh", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 'v':
      opt_verbose_level++;
      break;
    case 's':
      self_configure = false;
      break;
    case 'h':
      fmt::print(stderr, usage_msg, prog_name);
      exit(0);
      break;
    default:
      fmt::print(stderr, usage_msg, prog_name);
      exit(EXIT_FAILURE);
    }
  }
  auto index = optind;
  if (argc <= index) {
    fmt::print(stderr, usage_msg, prog_name);
    exit(EXIT_FAILURE);
  }
  remote_uri = argv[index++];
}

////////////////////////////////////////////////////////////////////
std::string
Bytes2Str(uint64_t bytes) {
  for (auto unit : {"B", "KB", "MB", "GB", "TB"}) {
    if (bytes < 1024) {
      // tsuba_mkfile assume no space between number and unit
      return fmt::format("{:d}{}", bytes, unit);
    }
    bytes >>= 10;
  }
  return "Invalid size";
}

void
DeleteFile(const std::string& dst_uri) {
  if (opt_verbose_level > 0) {
    fmt::print(" DeleteFile: {}\n", dst_uri);
  }
  auto dir_res = galois::ExtractDirName(dst_uri);
  if (!dir_res) {
    GALOIS_LOG_FATAL("FileDelete bad URI {}\n", dir_res.error());
  }
  std::unordered_set<std::string> files;
  files.emplace(galois::ExtractFileName(dst_uri));
  if (auto res = tsuba::FileDelete(dir_res.value(), files); !res) {
    GALOIS_LOG_FATAL(
        "FileDelete error [{}] sz: {} files[0]: {} err: {}\n", dir_res.value(),
        files.size(), files.size() > 0 ? *files.begin() : "", res.error());
  }
}

void
Mkfile(const std::string& dst_uri, uint64_t size) {
  std::vector<uint8_t> buf(size);
  init_data(buf.data(), size);
  if (opt_verbose_level > 0) {
    fmt::print(" Mkfile {}: {}\n", dst_uri, Bytes2Str(size));
  }
  if (auto res = tsuba::FileStore(dst_uri, buf.data(), size); !res) {
    GALOIS_LOG_FATAL("FileStore error {}\n", res.error());
  }
}

int
FileExists(const std::string& uri, uint64_t* size = nullptr) {
  tsuba::StatBuf stat_buf;
  if (auto res = tsuba::FileStat(uri, &stat_buf); !res) {
    if (opt_verbose_level > 0) {
      fmt::print(" Stat failed {}: {}\n", uri, res.error());
    }
    return 0;
  }
  if (size) {
    *size = stat_buf.size;
  }
  return 1;
}

void
Cp(const std::string& dst_uri, const std::string& src_uri) {
  uint64_t size{UINT64_C(0)};
  if (!FileExists(src_uri, &size)) {
    GALOIS_LOG_FATAL("Cannot stat {}\n", src_uri);
  }

  if (opt_verbose_level > 0) {
    fmt::print(" Cp {} to {}\n", src_uri, dst_uri);
  }
  std::vector<uint8_t> vec(size);
  auto buf_res = tsuba::FileGet(src_uri, vec.data(), UINT64_C(0), size);
  if (!buf_res) {
    GALOIS_LOG_FATAL("Failed get {} start 0 size {:#x}\n", src_uri, size);
  }

  if (auto res = tsuba::FileStore(dst_uri, vec.data(), size); !res) {
    GALOIS_LOG_FATAL("FileStore error {}\n", res.error());
  }
}

///////////////////////////////////////////////////////////////////

std::string
DoMD5(const std::string& path) {
  constexpr uint64_t read_block_size = (1 << 29);
  tsuba::StatBuf stat_buf;
  if (auto res = tsuba::FileStat(path, &stat_buf); !res) {
    GALOIS_LOG_FATAL("\n  Cannot stat {}\n", path);
  }

  MD5 md5;
  std::vector<uint8_t> vec;
  uint64_t size;
  for (uint64_t so_far = UINT64_C(0); so_far < stat_buf.size;
       so_far += read_block_size) {
    size = std::min(read_block_size, (stat_buf.size - so_far));
    vec.reserve(size);
    auto res = tsuba::FileGet(path, vec.data(), so_far, size);
    if (!res) {
      GALOIS_LOG_FATAL(
          "\n  Failed mmap start {:#x} size {:#x} total {:#x}\n", so_far, size,
          stat_buf.size);
    }
    md5.add(vec.data(), size);
  }
  return fmt::format("{}", md5.getHash());
}

///////////////////////////

struct Test {
  std::string name_;
  std::function<void()> func_;
  Test(const std::string& name, std::function<void()> func)
      : name_(name), func_(func) {}
};

std::pair<std::vector<std::string>, std::vector<uint64_t>>
ListDir(const std::string& dir) {
  std::vector<std::string> files;
  std::vector<uint64_t> size;
  auto fut = tsuba::FileListAsync(dir, &files, &size);
  if (auto res = fut.get(); !res) {
    GALOIS_LOG_FATAL("Bad return from ListAsync: {}", res.error());
  }
  return std::make_pair(files, size);
}

void
TestDir(const std::string& file, uint64_t num_bytes) {
  auto dir = galois::ExtractDirName(file);
  GALOIS_LOG_ASSERT(dir);
  auto [files, size] = ListDir(dir.value());
  GALOIS_LOG_ASSERT(files.size() == 1);
  GALOIS_LOG_ASSERT(files[0] == galois::ExtractFileName(file));
  GALOIS_LOG_ASSERT(size.size() == 1);
  GALOIS_LOG_ASSERT(size[0] == num_bytes);
}
void
MkCpSumLocal(
    uint64_t num_bytes, const std::string& local, const std::string& remote,
    std::vector<Test>& tests) {
  std::string bytes_str = Bytes2Str(num_bytes);
  tests.push_back(Test(
      fmt::format("Make local, copy (get), delete ({})", bytes_str), [=]() {
        GALOIS_LOG_ASSERT(!FileExists(local));
        GALOIS_LOG_ASSERT(!FileExists(remote));
        Mkfile(local, num_bytes);
        Cp(remote, local);

        GALOIS_LOG_ASSERT(FileExists(local));
        GALOIS_LOG_ASSERT(FileExists(remote));
        TestDir(local, num_bytes);
        TestDir(remote, num_bytes);

        auto m1 = DoMD5(local);
        auto m2 = DoMD5(remote);
        GALOIS_LOG_ASSERT(m1 == m2);

        DeleteFile(local);
        DeleteFile(remote);
        GALOIS_LOG_ASSERT(!FileExists(local));
        GALOIS_LOG_ASSERT(!FileExists(remote));
      }));
}

void
MkCpSumRemote(
    uint64_t num_bytes, const std::string& local, const std::string& remote,
    std::vector<Test>& tests) {
  std::string bytes_str = Bytes2Str(num_bytes);
  tests.push_back(Test(
      fmt::format("Make remote, copy (get) local, delete ({})", bytes_str),
      [=]() {
        GALOIS_LOG_ASSERT(!FileExists(local));
        GALOIS_LOG_ASSERT(!FileExists(remote));
        Mkfile(remote, num_bytes);
        Cp(local, remote);

        GALOIS_LOG_ASSERT(FileExists(local));
        GALOIS_LOG_ASSERT(FileExists(remote));
        TestDir(local, num_bytes);
        TestDir(remote, num_bytes);

        auto m1 = DoMD5(local);
        auto m2 = DoMD5(remote);
        GALOIS_LOG_ASSERT(m1 == m2);

        DeleteFile(local);
        DeleteFile(remote);
        GALOIS_LOG_ASSERT(!FileExists(local));
        GALOIS_LOG_ASSERT(!FileExists(remote));
      }));
}

void
DirPrefixRemote(
    uint64_t num_files, std::vector<std::string> fnames,
    const std::string& remote_dir, std::vector<Test>& tests) {
  constexpr int file_size = 16;
  tests.push_back(Test(
      fmt::format("Create, list, delete many files in ({})", remote_dir),
      [=]() {
        auto [files, size] = ListDir(remote_dir);
        GALOIS_LOG_ASSERT(files.size() == 0);
        GALOIS_LOG_ASSERT(size.size() == 0);
        for (uint64_t i = 0; i < num_files; ++i) {
          Mkfile(galois::JoinPath(remote_dir, fnames[i]), file_size);
        }
        {
          auto [files, size] = ListDir(remote_dir);
          if (opt_verbose_level > 0) {
            fmt::print(
                "Dir: {} files: {} Byte size: {}\n", remote_dir, files.size(),
                std::accumulate(
                    size.begin(), size.end(), decltype(size)::value_type(0)));
          }
          GALOIS_LOG_ASSERT(files.size() == num_files);
          GALOIS_LOG_ASSERT(size.size() == num_files);

          GALOIS_LOG_ASSERT(
              std::accumulate(
                  size.begin(), size.end(), decltype(size)::value_type(0)) ==
              num_files * file_size);

          for (uint64_t i = 0; i < num_files; ++i) {
            DeleteFile(galois::JoinPath(remote_dir, fnames[i]));
          }
        }
        {
          auto [files, size] = ListDir(remote_dir);
          GALOIS_LOG_ASSERT(files.size() == 0);
          GALOIS_LOG_ASSERT(size.size() == 0);
        }
      }));
}

std::vector<Test>
ConstructTests(std::string local_dir, std::string remote_dir) {
  std::vector<Test> tests;
  std::string rnd_str = galois::RandomAlphanumericString(12);
  std::string local_rnd = galois::JoinPath(local_dir, "ci-test-" + rnd_str);
  std::string remote_rnd = galois::JoinPath(remote_dir, "ci-test-" + rnd_str);

  // Each of these could be done on a different thread
  MkCpSumLocal(8, local_rnd, remote_rnd, tests);
  MkCpSumLocal(UINT64_C(1) << 13, local_rnd, remote_rnd, tests);
  MkCpSumLocal(UINT64_C(1) << 15, local_rnd, remote_rnd, tests);

  MkCpSumRemote(15, local_rnd, remote_rnd, tests);
  MkCpSumRemote((UINT64_C(1) << 13) - 1, local_rnd, remote_rnd, tests);
  MkCpSumRemote((UINT64_C(1) << 15) - 1, local_rnd, remote_rnd, tests);

  // Create a repository of random names
  constexpr int fnum = 5107;
  std::vector<std::string> fnames(fnum);
  for (auto& fname : fnames) {
    fname = galois::RandomAlphanumericString(12);
  }

  DirPrefixRemote(55, fnames, remote_dir, tests);
  // GS compatability mode is limited to pseudo-directories with 1,000 items
  // DirPrefixRemote(1010, fnames, remote_dir, tests);
  // DirPrefixRemote(fnum, fnames, remote_dir, tests);

  return tests;
}

int
main(int argc, char* argv[]) {
  int main_ret = 0;
  ParseArguments(argc, argv);

  if (self_configure) {
    // Add bin to path for manual testing
    std::string path = getenv("PATH");
    path.insert(0, "bin:");
    setenv("PATH", path.c_str(), 1);
  }
  auto uri_res = galois::Uri::Make(remote_uri);
  if (!uri_res) {
    GALOIS_LOG_FATAL("bad remote_uri: {}: {}", remote_uri, uri_res.error());
  }
  auto uri = uri_res.value();
  if (auto init_good = tsuba::Init(uri.scheme()); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }

  auto unique_result = galois::CreateUniqueDirectory(local_uri);
  GALOIS_LOG_ASSERT(unique_result);
  std::string tmp_dir(std::move(unique_result.value()));
  GALOIS_LOG_VASSERT(
      !FileExists(remote_uri), "Remote URI must not exist at start of test");
  auto tests = ConstructTests(tmp_dir, remote_uri);

  // Create annoyance files
  auto dir = remote_uri;
  while (dir[dir.size() - 1] == '/') {
    dir.pop_back();
  }
  for (char ch = 'a'; ch < 'z' + 1; ++ch) {
    Mkfile(dir + ch, 0);
    GALOIS_LOG_ASSERT(FileExists(dir + ch));
  }

  for (auto const& test : tests) {
    if (opt_verbose_level > 0) {
      fmt::print("Running: {}\n", test.name_);
    }
    test.func_();
  }

  for (char ch = 'a'; ch < 'z' + 1; ++ch) {
    DeleteFile(dir + ch);
    GALOIS_LOG_ASSERT(!FileExists(dir + ch));
  }

  boost::filesystem::remove_all(tmp_dir);
  return main_ret;
}

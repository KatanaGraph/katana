// Test every libtsuba::file.h routine except get/put async functions which are tested by tsuba_bench
#include "tsuba/tsuba.h"

#include <getopt.h>

#include <vector>

#include "bench_utils.h"
#include "galois/Logging.h"
#include "galois/Random.h"
#include "galois/Uri.h"
#include "md5.h"
#include "tsuba/file.h"

// remove_all (rm -rf) is just too sweet
#include <boost/filesystem.hpp>

int opt_verbose_level{0};
int opt_test_level{0};
std::string local_dir = "/tmp";
std::string local_prefix = "tsuba_test";
galois::Uri remote_dir;
std::string prog_name = "tsuba_test";
std::string usage_msg =
    "Usage: {} [options] <remote uri directory>\n"
    "  [-t] more tests, can be repeated (default=0)\n"
    "  [-v] verbose, can be repeated (default=false)\n"
    "  [-h] usage message\n";

void
ParseArguments(int argc, char* argv[]) {
  int c;
  int option_index = 0;
  while (1) {
    static struct option long_options[] = {};
    c = getopt_long(argc, argv, "vth", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 'v':
      opt_verbose_level++;
      break;
    case 't':
      opt_test_level++;
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
  auto cmd_uri = argv[index++];
  auto uri_res = galois::Uri::Make(cmd_uri);
  if (!uri_res) {
    GALOIS_LOG_FATAL("bad URI on command line {}\n", cmd_uri);
  }
  remote_dir = uri_res.value();
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
DeleteFiles(
    const galois::Uri& dir, const std::unordered_set<std::string>& files) {
  if (opt_verbose_level > 0) {
    fmt::print(
        " DeleteFiles dir: {} count: {} files[0]: {}\n", dir, files.size(),
        files.size() > 0 ? *files.begin() : "");
  }
  if (auto res = tsuba::FileDelete(dir.string(), files); !res) {
    GALOIS_LOG_FATAL(
        "FileDelete error [{}] sz: {} files[0]: {} err: {}\n", dir,
        files.size(), files.size() > 0 ? *files.begin() : "", res.error());
  }
}

void
DeleteFile(const galois::Uri& path) {
  if (opt_verbose_level > 0) {
    fmt::print(" DeleteFile: {}\n", path);
  }
  auto dir = path.DirName();
  std::unordered_set<std::string> files;
  files.emplace(path.BaseName());
  if (auto res = tsuba::FileDelete(dir.string(), files); !res) {
    GALOIS_LOG_FATAL(
        "FileDelete error [{}] sz: {} files[0]: {} err: {}\n", dir,
        files.size(), files.size() > 0 ? *files.begin() : "", res.error());
  }
}

void
Mkfile(const galois::Uri& path, uint64_t size) {
  std::vector<uint8_t> buf(size);
  init_data(buf.data(), size);
  if (opt_verbose_level > 0) {
    fmt::print(" Mkfile {}: {}\n", path, Bytes2Str(size));
  }
  if (auto res = tsuba::FileStore(path.string(), buf.data(), size); !res) {
    GALOIS_LOG_FATAL("FileStore error {}\n", res.error());
  }
}

int
FileExists(const galois::Uri& file, uint64_t* size = nullptr) {
  tsuba::StatBuf stat_buf;
  if (auto res = tsuba::FileStat(file.string(), &stat_buf); !res) {
    if (opt_verbose_level > 0) {
      fmt::print(" Stat failed {}: {}\n", file, res.error());
    }
    return 0;
  }
  if (size) {
    *size = stat_buf.size;
  }
  return 1;
}

void
Cp(const galois::Uri& dst, const galois::Uri& src) {
  uint64_t size{UINT64_C(0)};
  if (!FileExists(src, &size)) {
    GALOIS_LOG_FATAL("Cannot stat {}\n", src);
  }

  if (opt_verbose_level > 0) {
    fmt::print(" Cp {} to {}\n", src, dst);
  }
  std::vector<uint8_t> vec(size);
  auto buf_res = tsuba::FileGet(src.string(), vec.data(), UINT64_C(0), size);
  if (!buf_res) {
    GALOIS_LOG_FATAL("Failed get {} start 0 size {:#x}\n", src, size);
  }

  if (auto res = tsuba::FileStore(dst.string(), vec.data(), size); !res) {
    GALOIS_LOG_FATAL("FileStore error {}\n", res.error());
  }
}

std::string
DoMD5(const galois::Uri& path) {
  constexpr uint64_t read_block_size = (1 << 29);
  tsuba::StatBuf stat_buf;
  if (auto res = tsuba::FileStat(path.string(), &stat_buf); !res) {
    GALOIS_LOG_FATAL("\n  Cannot stat {}\n", path);
  }

  MD5 md5;
  std::vector<uint8_t> vec;
  uint64_t size;
  for (uint64_t so_far = UINT64_C(0); so_far < stat_buf.size;
       so_far += read_block_size) {
    size = std::min(read_block_size, (stat_buf.size - so_far));
    vec.reserve(size);
    auto res = tsuba::FileGet(path.string(), vec.data(), so_far, size);
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

void
PrintVec(std::vector<std::string> vec) {
  std::for_each(
      vec.cbegin(), vec.cend(), [](const auto& e) { fmt::print("  {}\n", e); });
}

std::pair<std::vector<std::string>, std::vector<uint64_t>>
ListDir(const galois::Uri& dir) {
  std::vector<std::string> files;
  std::vector<uint64_t> size;
  auto fut = tsuba::FileListAsync(dir.string(), &files, &size);
  if (auto res = fut.get(); !res) {
    GALOIS_LOG_FATAL("Bad return from ListAsync: {}", res.error());
  }
  return std::make_pair(files, size);
}

void
TestDir(const galois::Uri& file, uint64_t num_bytes) {
  // GS (and S3?) require pseudo-directory names to end with /
  auto dir = file.DirName() + galois::Uri::kSepChar;
  auto [files, size] = ListDir(dir);
  if (opt_verbose_level > 0) {
    fmt::print(" Listing {} numFiles: {}\n", dir, files.size());
    PrintVec(files);
  }
  GALOIS_LOG_ASSERT(files.size() == 1);
  GALOIS_LOG_ASSERT(files[0] == file.BaseName());
  GALOIS_LOG_ASSERT(size.size() == 1);
  GALOIS_LOG_ASSERT(size[0] == num_bytes);
}
void
MkCpSumLocal(
    uint64_t num_bytes, const galois::Uri& local, const galois::Uri& remote,
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
    uint64_t num_bytes, const galois::Uri& local, const galois::Uri& remote,
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
    const galois::Uri& remote_dir, std::vector<Test>& tests) {
  constexpr int file_size = 16;
  tests.push_back(Test(
      fmt::format("Create, list, delete many files in ({})", remote_dir),
      [=]() {
        auto [files, size] = ListDir(remote_dir);
        GALOIS_LOG_ASSERT(files.size() == 0);
        GALOIS_LOG_ASSERT(size.size() == 0);
        for (uint64_t i = 0; i < num_files; ++i) {
          Mkfile(remote_dir.Join(fnames[i]), file_size);
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

          std::unordered_set<std::string> file_set(
              files.cbegin(), files.cend());
          DeleteFiles(remote_dir, file_set);
        }
        {
          auto [files, size] = ListDir(remote_dir);
          GALOIS_LOG_ASSERT(files.size() == 0);
          GALOIS_LOG_ASSERT(size.size() == 0);
        }
      }));
}

std::vector<Test>
ConstructTests(const galois::Uri& local_dir, const galois::Uri& remote_dir) {
  std::vector<Test> tests;
  auto local_rnd = local_dir.RandFile("ci-test");
  auto remote_rnd = remote_dir.RandFile("ci-test");

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
  if (opt_test_level > 0) {
    // S3 batch operations might make this faster.  Bottleneck is file creation
    // https://docs.aws.amazon.com/AmazonS3/latest/user-guide/batch-ops-create-job.html
    DirPrefixRemote(fnum, fnames, remote_dir, tests);
  }

  return tests;
}

int
main(int argc, char* argv[]) {
  int main_ret = 0;
  ParseArguments(argc, argv);

  if (auto init_good = tsuba::Init(remote_dir.scheme()); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  auto local_uri_res = galois::Uri::Make(local_dir);
  GALOIS_LOG_ASSERT(local_uri_res);
  auto local_uri = local_uri_res.value();
  auto local_rand_dir = local_uri.RandFile(local_prefix);

  std::string tmp_dir(local_rand_dir.path());  // path for local file
  GALOIS_LOG_VASSERT(
      !FileExists(remote_dir), "Remote URI must not exist at start of test");
  auto tests = ConstructTests(local_rand_dir, remote_dir);

  // Create annoyance files
  auto dir = remote_dir.StripSep();
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

  std::unordered_set<std::string> file_set;
  for (char ch = 'a'; ch < 'z' + 1; ++ch) {
    file_set.emplace(dir.string() + ch);
  }
  DeleteFiles(dir.DirName(), file_set);
  // No assert that all files have disappeared

  boost::filesystem::remove_all(tmp_dir);
  return main_ret;
}

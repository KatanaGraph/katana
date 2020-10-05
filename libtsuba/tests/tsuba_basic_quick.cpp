// Run some quick, basic sanity checks on tsuba's file functionality
#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>

#include <vector>

#include "bench_utils.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "galois/Random.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

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
    GALOIS_LOG_FATAL("FileDelete error {}\n", res.error());
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

  auto buf_res = tsuba::FileMmap(src_uri, UINT64_C(0), size);
  if (!buf_res) {
    GALOIS_LOG_FATAL("Failed mmap {} start 0 size {:#x}\n", src_uri, size);
  }
  uint8_t* buf = buf_res.value();

  if (auto res = tsuba::FileStore(dst_uri, buf, size); !res) {
    GALOIS_LOG_FATAL("FileStore error {}\n", res.error());
  }
}
///////////////////////////////////////////////////////////////////

int
RunPopen(const std::string& cmd, std::string& out) {
  char buff[4096];
  FILE* fp = popen(cmd.c_str(), "r");
  if (fp == NULL) {
    GALOIS_LOG_ERROR("Cannot popen: {}", galois::ResultErrno());
    return -1;
  }
  while (fgets(buff, sizeof(buff), fp)) {
    out += buff;
  }
  pclose(fp);
  if (opt_verbose_level > 1) {
    fmt::print("out: {} cmd: {}\n", out, cmd);
  }
  return 0;
}

int
MD5sumRun(const std::string& cmd, std::string& out) {
  std::string cmd_out;
  int res = RunPopen(cmd, cmd_out);
  if (res != 0) {
    return res;
  }

  std::size_t first_space = cmd_out.find(' ');
  if (first_space == std::string::npos) {
    return -1;
  }
  out = cmd_out.substr(0, first_space);
  return 0;
}

int
RunMd5Check(const std::vector<std::string>& args) {
  std::vector<std::string> results;
  for (auto const& arg : args) {
    std::string out;
    int res = MD5sumRun(fmt::format("tsuba_md5sum {}", arg), out);
    if (res != 0) {
      fmt::print("Test FAILED: {}\n", arg);
      return -1;
    }
    results.push_back(out);
  }
  // Now test that they are all equal
  auto first = results[0];
  for (auto const& result : results) {
    if (first != result) {
      fmt::print(
          "Test FAILED, outputs\n  first: {}\n  other: {}\n", first, result);
      return -2;
    }
  }
  return 0;
}

///////////////////////////

struct Test {
  std::string name_;
  std::function<void()> func_;
  Test(const std::string& name, std::function<void()> func)
      : name_(name), func_(func) {}
};

void
MkCpSumLocal(
    uint64_t num_bytes, const std::string& local, const std::string& remote,
    std::vector<Test>& tests) {
  std::string bytes_str = Bytes2Str(num_bytes);
  tests.push_back(
      Test(fmt::format("Make local, copy, delete ({})", bytes_str), [=]() {
        Mkfile(local, num_bytes);
        Cp(remote, local);
        std::vector<std::string> args{local, remote};
        RunMd5Check(args);
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
      fmt::format("Make remote, copy local, delete ({})", bytes_str), [=]() {
        Mkfile(remote, num_bytes);
        Cp(local, remote);
        std::vector<std::string> args{local, remote};
        RunMd5Check(args);
        DeleteFile(local);
        DeleteFile(remote);
        GALOIS_LOG_ASSERT(!FileExists(local));
        GALOIS_LOG_ASSERT(!FileExists(remote));
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
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }

  auto unique_result = galois::CreateUniqueDirectory(local_uri);
  GALOIS_LOG_ASSERT(unique_result);
  std::string tmp_dir(std::move(unique_result.value()));
  auto tests = ConstructTests(tmp_dir, remote_uri);

  for (auto const& test : tests) {
    if (opt_verbose_level > 0) {
      fmt::print("Running: {}\n", test.name_);
    }
    test.func_();
  }

  boost::filesystem::remove_all(tmp_dir);
  return main_ret;
}

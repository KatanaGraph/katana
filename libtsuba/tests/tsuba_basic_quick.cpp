// Run some quick, basic sanity checks on tsuba's file functionality
#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>

#include <vector>

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

static constexpr auto chars =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
static auto chars_len = std::strlen(chars);

// 19 chars, with 1 null byte
void
get_time_string(char* buf, int32_t limit) {
  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buf, limit, "%Y/%m/%d %H:%M:%S ", timeinfo);
}

void
init_data(uint8_t* buf, int32_t limit) {
  if (limit < 0)
    return;
  if (limit < 19) {
    for (; limit; limit--) {
      *buf++ = 'a';
    }
    return;
  } else {
    char tmp[32];              // Generous with space
    get_time_string(tmp, 31);  // Trailing null
                               // Copy without trailing null
    memcpy(buf, tmp, 19);
    buf += 19;
    if (limit > 19) {
      *buf++ = ' ';
      uint64_t char_idx = UINT64_C(0);
      for (limit -= 20; limit; limit--) {
        *buf++ = chars[char_idx++ % chars_len];  // We could make this faster...
      }
    }
  }
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

enum class TestType {
  kSystem,
  kCall,
  kMDsum,
};

struct Test {
  TestType type_;
  std::string name_;
  std::vector<std::string> cmds_;
  std::function<void()> func_;
  Test(
      TestType type, const std::string& name, const std::string& cmd,
      std::function<void()> func)
      : type_(type), name_(name), func_(func) {
    cmds_.push_back(cmd);
  }
  Test(
      TestType type, const std::string& name,
      const std::vector<std::string>& cmds)
      : type_(type), name_(name), cmds_(cmds) {}
};

void
MkCpSumLocal(
    uint64_t num_bytes, const std::string& local, const std::string& remote,
    std::vector<Test>& tests) {
  std::string bytes_str = Bytes2Str(num_bytes);
  tests.push_back(Test(
      TestType::kCall, fmt::format("Make a local file ({})", bytes_str), "",
      [=]() { Mkfile(local, num_bytes); }));
  tests.push_back(Test(
      TestType::kCall, fmt::format("Copy local file to remote ({})", bytes_str),
      "", [=]() { Cp(remote, local); }));
  std::vector<std::string> cmds{
      fmt::format("tsuba_md5sum {}", local),
      fmt::format("tsuba_md5sum {}", remote)};
  tests.push_back(Test(
      TestType::kMDsum,
      fmt::format("Compare MD5sum of local and remote files ({})", bytes_str),
      cmds));
  tests.push_back(Test(
      TestType::kCall,
      fmt::format("Delete local and remote files ({})", bytes_str), "", [=]() {
        DeleteFile(local);
        DeleteFile(remote);
      }));
  tests.push_back(Test(
      TestType::kCall,
      fmt::format("Delete local and remote files ({})", bytes_str), "", [=]() {
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
      TestType::kCall, fmt::format("Make remote file ({})", bytes_str), "",
      [=]() { Mkfile(remote, num_bytes); }));
  tests.push_back(Test(
      TestType::kCall, fmt::format("Copy remote file to local ({})", bytes_str),
      "", [=]() { Cp(local, remote); }));
  std::vector<std::string> cmds{
      fmt::format("tsuba_md5sum {}", local),
      fmt::format("tsuba_md5sum {}", remote)};
  tests.push_back(Test(
      TestType::kMDsum,
      fmt::format("Compare MD5sum of local and remote files ({})", bytes_str),
      cmds));
  tests.push_back(Test(
      TestType::kCall,
      fmt::format("Delete local and remote files ({})", bytes_str), "", [=]() {
        DeleteFile(local);
        DeleteFile(remote);
      }));
  tests.push_back(Test(
      TestType::kCall,
      fmt::format("Delete local and remote files ({})", bytes_str), "", [=]() {
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
    switch (test.type_) {
    case TestType::kSystem: {
      int res = system(test.cmds_[0].c_str());
      if (res != 0) {
        main_ret = EXIT_FAILURE;
        fmt::print("Test FAILED\n");
        break;
      }
    } break;
    case TestType::kCall: {
      test.func_();
    } break;
    case TestType::kMDsum: {
      std::vector<std::string> results;
      for (auto const& cmd : test.cmds_) {
        std::string out;
        int res = MD5sumRun(cmd, out);
        if (res != 0) {
          main_ret = EXIT_FAILURE;
          fmt::print("Test FAILED\n");
          break;
        }
        results.push_back(out);
      }
      // Now test that they are all equal
      auto first = results[0];
      for (auto const& result : results) {
        if (first != result) {
          fmt::print(
              "Test FAILED, outputs\n  first: {}\n  other: {}\n", first,
              result);
          main_ret = EXIT_FAILURE;
          break;
        }
      }
    } break;
    default:
      fmt::print("Unknown test type {}\n", test.name_);
      main_ret = EXIT_FAILURE;
    }
  }

  boost::filesystem::remove_all(tmp_dir);
  return main_ret;
}
